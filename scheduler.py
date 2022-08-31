from multiprocessing import Process
from detector import Detector
from data_getter import DataGetter
import time

QUOTE_GETTER_CYCLE = 6
DATA_UPDATE_CYCLE = 30
DETECTOR_CYCLE = 15

class Scheduler():

    def __init__(self, FREQUENCY, MA):
        self.FREQUENCY = FREQUENCY
        self.MA = MA

    def schedule_realtime_quote_getter(self, cycle=QUOTE_GETTER_CYCLE):
        """
        周期性获取股票实时行情数据
        """
        getter = DataGetter(self.FREQUENCY, self.MA)
        while True:
            getter.store_ticks()
            time.sleep(cycle)
        
    def schedule_date_updater(self, cycle=DATA_UPDATE_CYCLE):
        """
        周期性更新分钟K线数据到数据库
        """
        getter = DataGetter(self.FREQUENCY, self.MA)
        while True:
            getter.update_all()
            time.sleep(cycle)

    def schedule_detector(self, cycle=DETECTOR_CYCLE):
        """
        周期性判断行情信号
        """
        detector = Detector(self.FREQUENCY, self.MA)
        while True:
            detector.run()
            time.sleep(cycle)

    def run(self):
        print("启动程序")
        quote_getter_process = Process(target=self.schedule_realtime_quote_getter)
        quote_getter_process.start()
        updater_process = Process(target=self.schedule_date_updater)
        updater_process.start()
        detector_process = Process(target=self.schedule_detector)
        detector_process.start()

if __name__ == '__main__':
    s = Scheduler()
    s.run()