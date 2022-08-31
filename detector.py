import tushare as ts
import pandas as pd
from database import Database
from data_getter import DataGetter
from datetime import datetime
from multiprocessing.pool import Pool
from tqdm import tqdm
import csv
import os

class Detector(object):
    def __init__(self, FREQUENCY, MA):
        self.FREQUENCY = FREQUENCY
        self.MA = MA
        self.data_getter = DataGetter(FREQUENCY, MA)
        self.first_run = True
        self.fieldnames = ['date', 'time', 'frequency', 'stock', 'ma', 'price', 'description']
        self.file_path = None
        self.create_file()

    def get_ma(self, stock_code):
        '''
        根据周期值FREQUENCY，和MA参数值来计算ma值
        '''
        df = self.data_getter.get_k_line_data(stock_code)
        # 把数据以下降形式重新排序，以便计算正确的ma值
        df_sorted = df.sort_values('time')
        ma_values = df_sorted['price'].rolling(window=self.MA).mean()
        # 返回最后一个MA值
        return ma_values[-1:].item()

    def cross_above_ma(self, ma, previous_price, current_price):
        '''
        判断K线是否上穿MA线
        '''
        if previous_price < ma and current_price > ma:
            return True
        else:
            return False

    def check(self, stock):
        # 判断今天是否有足够的实时分笔数据, 如果没有就忽略等下一次更新
        if self.data_getter.get_latest_prices(stock):
            # 获取该股票当前价格和时间日期
            price, time, date= self.data_getter.get_latest_prices(stock)
            ma = self.get_ma(stock)
            # 判断上穿信号， 并输出结果
            if self.cross_above_ma(ma, price['previous'], price['current']):
                self.print_result(date, time, stock, ma, price['current'])

    def create_file(self):
        """
        创建带标题的CSV文件
        """
        import os, csv
        date_created = datetime.now().strftime('%Y%m%d_%H%M%S')
        cwd = os.getcwd()
        save_path = os.path.join(cwd, 'csv/')
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        file_name = '{}.csv'.format(date_created)
        self.file_path = os.path.join(save_path, file_name)
        with open(self.file_path, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writeheader()
    
    def print_result(self, date, time, stock, ma, price):
        print('{} {} {} {} 上穿MA线'.format(date, time, stock, price))
        try:
            with open(self.file_path, 'a', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.fieldnames)
                writer.writerow({'date': date, 
                                 'time': time,
                                 'frequency': self.FREQUENCY,
                                 'stock': stock,
                                 'ma': ma,
                                 'price': price,
                                 'description': 'cross above'
                                })
        except Exception as e:
            print('CSV文件写入错误', e)

    def run(self):
        if self.first_run:
            quotes = self.data_getter.get_all_realtime_quotes()
            # 只在第一次运行保存股票代码
            self.stock_codes = list(quotes.index)
            # 解除第一次运行状态
            self.first_run = False
        time_start = datetime.now()
        with Pool() as pool:
            # print("开始判别数据")
            pool.map(self.check, self.stock_codes)
            # with tqdm(total=len(self.stock_codes)) as pbar:
            #     for _, _ in tqdm(enumerate(pool.imap_unordered(self.check, self.stock_codes))):
            #         pbar.update()
        time_finish = datetime.now() - time_start
        print('成功判别数据 - 所花时间: %s' % time_finish.seconds)
        
if __name__ == '__main__':
    detector = Detector()
    detector.run()
