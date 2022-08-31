from scheduler import Scheduler
from data_getter import DataGetter
import time
import tushare as ts
import datetime
from validator import Validator

TOKEN = 'Hello world!'

def get_freq_input():
    print('D=日k线 1=1分钟 5=5分钟 15=15分钟 30=30分钟 60=60分钟')
    frequency_list = ['D', '1', '5', '15', '30', '60']
    while True:
        input_frequency = str(input('请输入周期值:'))
        if input_frequency in frequency_list:
            break
        else:
            print('无效周期值! ', input_frequency)
    return input_frequency
        

def get_ma_input():
    while True:
        input_ma = int(input('请输入MA参数值（5-250):'))
        if input_ma >= 5 and input_ma <=250:
            break
        else:
            print('无效MA参数值!')
    return input_ma

def main(): 
    validator = Validator()
    token = validator.get_token()
    if not token == TOKEN:
        print("验证失败")
        return
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    # 获取交易日状态 （isOpen为1就是交易日）
    open_list = ts.trade_cal()
    is_today_open = open_list.isOpen[open_list.calendarDate == today]
    is_today_open = is_today_open.values[0]
    # 判断今天是否交易日
    if is_today_open == 1:
        frequency = get_freq_input()
        ma = get_ma_input()
        data = DataGetter(frequency, ma)
        data.prepare_hist_data()
        time_now = datetime.datetime.now().strftime('%H%M%S')
        # 判断现在是否是交易时间 （09:30 - 11:30, 13:00 - 15:00）
        if 93000 < int(time_now) < 113000 or 130000 < int(time_now) < 180000:
            scheduler = Scheduler(frequency, ma)
            scheduler.run()
        else:
            print("现在是休市时段， 请稍后再试")
            pass
    else:
        print("今天休市")
        pass
        
if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    main()
