import tushare as ts
from datetime import date, datetime, timedelta
import pandas as pd
from database import Database
from math import ceil
from multiprocessing.pool import Pool
from tqdm import tqdm
from pandas.core.frame import DataFrame
from config import TOKEN

class DataGetter(object):
    database = Database()
    def __init__(self, FREQUENCY, MA):
        self.FREQUENCY = FREQUENCY
        self.MA = MA
        self.table_name = self.database.get_tablename(FREQUENCY)

    def get_today_ticks(self, code):
        '''
        获取今天的分笔数据
        '''
        try: 
            df=ts.get_today_ticks(str(code), retry_count=3, pause=0.008)
            return df
        except Exception as e: 
            print(e)
            return None

    def get_tick_data(self, code, date):
        """
        获取历史分笔数据 (最多只能获取前10个交易日的数据)
        """
        try:
            df = ts.get_tick_data(code, date=date, src='tt')
            return df
        except Exception as e: 
            # print(e)
            return None

    def get_all_company_info(self):
        """
        获取所有上市公司基本资料
        """
        api = ts.pro_api(TOKEN)
        df = api.stock_basic(exchange='', 
                            list_status='L',
                            fields='ts_code,symbol,name,list_date')
        # print("成功获取%d家公司资料" % len(df))
        return(df)

    def get_all_code(self):
        """
        获取所有上市公司代码列表
        """
        df = self.get_all_company_info()
        codes = df["symbol"]
        return codes.tolist()

    def get_range_groups(self, total, each_num):
        """
        把总数量割成几等份
        """
        result = []
        start = 0
        end = each_num
        for _ in range(0, total // each_num):
            # print(start, " ", end)
            result.append((start, end))
            start = end
            end += each_num
        result.append((start, start + total % each_num))
        return result

    def get_realtime_quotes(self, codes):
        """
        获取所有上市公司的实时行情数据
        """
        num_stocks = len(codes)
        num_each_group = 880
        stock_groups = self.get_range_groups(num_stocks, num_each_group)
        data_frames = []
        # time_start= datetime.now()
        for group in stock_groups:
            start = group[0]
            end = group[1]
            # print("正在获取第%d-%d家公司" % (start, end))
            df = ts.get_realtime_quotes(codes[start:end])
            data_frames.append(df)
        # time_finish = datetime.now()
        result = pd.concat(data_frames)
        # print(result[['code','name','price','time']])
        # print('开始时间：' + str(time_start))
        # print('完成时间：' + str(time_finish))
        return result

    def get_history(self, code):
        """
        获取个股历史交易数据（包括均线数据）
        """
        # get_hist_data()不输入日期情况下， 直接返回350行数据
        df = ts.get_hist_data(code, ktype=self.FREQUENCY)
        if isinstance(df, DataFrame) and not df.empty:
            # 处理每一行的数据
            for index, _ in df.iterrows():
                # 处理日数据
                if self.FREQUENCY == 'D':
                    df.loc[index, 'time'] = 0
                    df.loc[index, 'date'] = index.replace('-', '')
                # 处理分钟数据
                else: 
                    # 把index列的timestamp拆成日期和时间， 并保存到两个新的列
                    time = index.split(' ')[1]
                    date = index.split(' ')[0]
                    df.loc[index, 'time'] = time
                    df.loc[index, 'date'] = date.replace('-', '')
                    # 添加股票代码到每一行数据
                df.loc[index, 'stock_code'] = code           
            return df

    def get_1min_history(self, code, date):
        """
        获取某天的分钟K线数据
        """
        # 获取当天的分笔历史数据
        tick_data = self.get_tick_data(code, date)
        # 检测是否有返回数据（例如可能当天该股票停牌）
        if isinstance(tick_data, DataFrame) and not tick_data.empty:
            return self.get_min_data(tick_data, date, '1')
        else:
            return None

    def get_min_data(self, tick_data, date, frequency):
        '''
        把分笔数据转化成分钟K线数据
        : tick_data: 分笔数据 （dataframe格式）
        : date: 数据日期
        : frequency: K线周期
        '''
        if frequency == '1' or frequency == '5' or frequency == '15' or frequency == '30' or frequency == '60':
            if isinstance(tick_data, DataFrame):
                if not tick_data.empty:
                    # 把time设置为index
                    df = tick_data.set_index('time', drop = False)
                    # 在每条数据加上date_time, 例如 20181227 14:42:00
                    for i, _ in df.iterrows():
                        df.loc[i, "date_time"] = date + ' ' + i
                    # 把date_time设置为index
                    df = df.set_index('date_time', drop = False)
                    # 把date_time转化成pandas的datetime格式， 例如2018-12-27 14:42:00
                    df.index = pd.to_datetime(df.index)
                    # 定义周期
                    rule = '{}min'.format(frequency)
                    # 根据Index, 抽取每周期最后一行数据
                    df2 = df.resample(rule).last() 
                    # 把没有数据的行去掉
                    return df2.dropna()
            else:
                # print('get_min_data - 参数不是Dataframe格式 ')
                return None
        else: 
            print('get_min_data - 无效周期 ')

    def store_1min_data(self, code, type='history'):
        """
        保存1分钟K线数据到数据库
        : code: 股票代码
        : type: 数据源类型（历史或今天的数据）
        """
        date_str = self.last_trade_date
        # 尝试次数（避免死循环）
        count = 60
        while count > 0:
            df = self.get_1min_history(code, date_str)
            # 如果返回数据为None, 需要继续轮询查找上一个交易日
            # 例如原因可能是该股票当天停牌

            if not isinstance(df, DataFrame) and not df:
                date_obj = datetime.strptime(date_str, '%Y%m%d') 
                date_str = self.get_last_trade_date(date_obj)
                count -= 1
            else:
                break

        # 检测数据是否有效
        if isinstance(df, DataFrame) and not df.empty:
            # 把该股票的1分钟K线数据存到数据库
            for index, row in df.iterrows():
                stock_code = code
                # trade_date = index.strftime('%Y%m%d')
                time = index.strftime('%H:%M:%S')
                price = row['price']
                # print(self.table_name, stock_code, date_str, time, price)
                self.database.insert(table_name=self.table_name,
                                        stock_code=stock_code, 
                                        date=date_str, 
                                        time=time, 
                                        price=price)
            # print("成功保存1分钟数据 %s" % stock_code) 
    
    def store_history_data(self, code):
        """
        把一个股票5、10、15、30、60分钟、或日的K线数据保存到数据库
        """
        df = self.get_history(code)
        if isinstance(df, DataFrame) and not df.empty:
            for _, row in df.iterrows():
                stock_code = row['stock_code']
                date = row['date']
                time = row['time']
                price = row['close']
                if self.table_name == None:
                    print("错误： 无效分钟周期")
                    return
                self.database.insert(table_name=self.table_name,
                                    stock_code=stock_code,
                                    date=date,
                                    time=time,
                                    price=price)
    def get_k_line_data(self, stock_code):
        """
        根据MA参数值和FREQUENCY周期，获取对应的K线数据
        MA参数值决定需要调用多少行数据
        """
        df = self.database.get_dataframe(self.table_name, stock_code, self.MA)
        return df

    def get_last_trade_date(self, date):
        """
        获取上一个交易日日期
        : date: 目标日期
        : return: 最上一个交易日的字符串
        """
        # 获取交易日状态 （isOpen为1就是交易日）
        open_list = ts.trade_cal()
        if isinstance(open_list, DataFrame) and not open_list.empty:
            # target_date = date.today() - timedelta(days=1)
            last_trade_date = date - timedelta(days=1)
            # 循环找出上一个交易日
            while True:
                last_trade_date_string = last_trade_date.strftime('%Y-%m-%d')
                is_date_open = open_list.isOpen[open_list.calendarDate == last_trade_date_string]
                is_date_open = is_date_open.values[0]
                if is_date_open == 1:
                    return last_trade_date.strftime('%Y%m%d')
                else:
                    last_trade_date -= timedelta(days=1)

    def prepare_hist_data(self):
        """
        根据不同周期，把昨天以前的历史K线数据
        存到本地数据库，方便调用和计算
        """
        print('正在获取历史k线数据......')
        if self.FREQUENCY == '1':
            '''
            获取昨天的历史分笔数据，并转化成分钟K线数据保存到数据库
            '''
            self.last_trade_date = self.get_last_trade_date(date.today())
            all_codes = self.get_all_code()
            # time_start = datetime.now()
            with Pool() as pool:
                print('开始加载历史数据')
                with tqdm(total=len(all_codes), ncols=75) as pbar:
                    for _, _ in tqdm(enumerate(pool.imap_unordered(self.store_1min_data, all_codes))):
                        pbar.update()
            # 计算所花时间
            # time_finish = datetime.now() - time_start
            # print('完成时间 {} 秒'.format(time_finish.seconds))

        elif self.FREQUENCY == '5' or self.FREQUENCY== '15' or self.FREQUENCY == '30' or self.FREQUENCY == '60' or self.FREQUENCY == 'D':
            '''
            获取1分钟以外的分钟历史数据，保存到数据库
            '''
            all_codes = self.get_all_code()
            # time_start = datetime.now()
            with Pool() as pool:
                print('开始加载历史数据')
                with tqdm(total=len(all_codes), ncols=75) as pbar:
                    for i, _ in tqdm(enumerate(pool.imap_unordered(self.store_history_data, all_codes))):
                        pbar.update()
            # time_finish = datetime.now() - time_start
            # print('完成时间 {} 秒'.format(time_finish.seconds))

        else:
            print("错误: 无效周期值")
            # self.database.close()
            return
        # self.database.close()
        print("成功获取数据并保存到数据库！")

    def get_all_realtime_quotes(self):
        '''
        获取实时价格数据
        : return: 返回dataframe数据（code, price, time)
        '''
        all_codes = self.get_all_code()
        df = self.get_realtime_quotes(all_codes)
        # 把股票代码设置为index
        df = df.set_index('code')
        return df.loc[:, ['price', 'time']]
    
    def store_ticks(self):
        '''
        把所有股票的实时行情保存到数据库
        '''
        time_start = datetime.now()
        self.database.reconnect()
        df = self.get_all_realtime_quotes()
        date = datetime.today().strftime('%Y%m%d')
        if isinstance(df, DataFrame) and not df.empty:
            for index, row in df.iterrows():
                # trade_date = index.strftime('%Y%m%d')
                stock_code = index
                time = row['time']
                price = row['price']
                self.database.insert(table_name='realtime_quotes',
                                    stock_code=stock_code, 
                                    date=date, 
                                    time=time, 
                                    price=price)
        # self.database.close()
        time_finish = datetime.now() - time_start
        print('成保存实时行情 - 所花时间: %s' % time_finish.seconds)

    def update_today_minute_data(self, stock_code):
        """
        保存和更新分钟周期数据
        """
        date = datetime.today().strftime('%Y%m%d')
        # 从数据库获取今天的实时行情记录
        df = self.database.get_realtime_quotes_record(stock_code, date)
        if isinstance(df, DataFrame) and not df.empty:
            # 把time设置为index
            df = df.set_index('time', drop = False)
            # 在每条数据加上date_time, 例如 20181227 14:42:00
            for i, _ in df.iterrows():
                df.loc[i, "date_time"] = date + ' ' + i
            # 把date_time设置为index
            df = df.set_index('date_time', drop = False)
            # 把date_time转化成pandas的datetime格式， 例如2018-12-27 14:42:00
            df.index = pd.to_datetime(df.index)
            # 定义周期
            rule = '{}min'.format(self.FREQUENCY)
            # 根据Index, 抽取每周期最后一行数据
            df2 = df.resample(rule).last() 
            # 把缺失的分钟数据的行去掉
            df2 = df2.dropna()
            for index, row in df2.iterrows():
                stock_code = row['stock_code']
                date = row['date']
                time = index.strftime('%H:%M:%S')
                price = row['price']
                self.database.insert(table_name=self.table_name,
                                        stock_code=stock_code, 
                                        date=date, 
                                        time=time, 
                                        price=price)

    def get_latest_prices(self, stock_code):
        '''
        获取最后两个实时价格, 和最后一个价格的时间
        ''' 
        date = datetime.today().strftime('%Y%m%d')
        df = self.database.get_realtime_quotes_record(stock_code, date, lastest=True)
        if isinstance(df, DataFrame) and not df.empty:
            df = DataFrame(df.set_index('stock_code'))
            price = {}
            if len(df.index) == 2:
                price['current'] = df.iloc[0]['price']
                price['previous'] = df.iloc[1]['price']
                time = df.iloc[0]['time']
                date = df.iloc[0]['date']
                return price, time, date
            else:
                # 如果没有两个数据，代表程序今天没有获取足够实时分笔数据
                return None

    def update_all(self):
        '''
        把分钟周期数据更新到数据库
        '''
        self.database.reconnect()
        # 获取所有股票代码
        all_codes = self.get_all_code()
        # 多线程更新今天的分钟K线数据
        time_start = datetime.now()
        with Pool() as pool:
            # print("\n开始更新分钟数据")
            pool.map(self.update_today_minute_data, all_codes)
            # with tqdm(total=len(all_codes)) as pbar:
            #     for i, _ in tqdm(enumerate(pool.imap_unordered(self.update_today_minute_data, all_codes))):
            #         pbar.update()
        time_finish = datetime.now() - time_start
        print('成功更新分钟数据 - 所花时间: %s' % time_finish.seconds)
        # 更新数据后关闭数据库
        # self.database.close()

    def run(self): 
        pass

if __name__ == '__main__':
    getter = DataGetter()
    getter.store_ticks()
    getter.update_all()