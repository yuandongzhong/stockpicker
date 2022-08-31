import sqlite3
import pandas as pd

class Database():
    """
    通过本地数据库， 保存不同周期的K线数据
    """
    table_names = ['1min', '5min', '15min', '30min', '60min', 'day', 'realtime_quotes']
    sqlite_file = 'db.sqlite'

    def __init__(self):
        try:
            self.conn = sqlite3.connect(self.sqlite_file, timeout=10.0)
            self.c = self.conn.cursor()
            self.create_tables()
            self.create_index()
        except sqlite3.Error as e:
            print('数据库初始化错误: {}'.format(e))

    def reconnect(self):
        try:
            self.conn = sqlite3.connect(self.sqlite_file, timeout=10.0)
            self.c = self.conn.cursor()
        except sqlite3.Error as e:
            print('数据库连接错误: {}'.format(e)) 

    def create_tables(self):
        """
        创建数据表（如果数据表已经存在， 就忽略）
        """
        for table_name in self.table_names:
            self.c.execute('''CREATE TABLE IF NOT EXISTS '{}' (
                            stock_code text NOT NULL,
                            date text NOT NULL,
                            time text NOT NULL,
                            price real NOT NULL,
                            CONSTRAINT name_unique 
                            UNIQUE (stock_code, date, time) 
                            ON CONFLICT IGNORE);'''.format(table_name))

    def create_index(self): 
        """
        添加索引
        """
        query = '''CREATE INDEX IF NOT EXISTS idx_stock_and_date 
                   ON '{}' (stock_code, date, time);'''
        for table_name in self.table_names:
            try:
                self.c.execute(query.format(table_name))
            except Exception as e:
                print("Index Creation Error: {}".format(e))

    def insert(self, table_name, stock_code, date, time, price):
        query = '''INSERT INTO '{table_name}' (stock_code, date, time, price) 
                   VALUES ('{stock_code}', {date}, '{time}', {price});'''\
                   .format(table_name=table_name, 
                           stock_code=stock_code, 
                           date=date, 
                           time=time, 
                           price=price)
        try:
            self.c.execute(query)
            self.conn.commit()
        except Exception as e:
            print("数据库写入错误：{}".format(e))
            self.conn.rollback()

    def get_dataframe(self, table, stock, row_num):
        '''
        从数据库调取数据并转化成dataframe格式
        注意排序为时间最近的数据
        '''
        query = '''SELECT * FROM '%s' 
                   WHERE stock_code = '%s' 
                   ORDER BY date DESC, 
                   time DESC 
                   limit %d;''' % (table, stock, row_num)
        return pd.read_sql_query(query, self.conn)

    def get_realtime_quotes_record(self, stock, date, lastest=False):
        '''
        从数据库获取实时行情的记录数据
        : stock: 股票代码
        : date: 日期
        : lastest: 是否获取最新的两个行情，默认False
        ''' 
        table = 'realtime_quotes'   
        # 获取该股票当天的所有分笔数据
        if not lastest:
            query = '''SELECT * FROM '%s' 
                    WHERE stock_code = '%s' 
                    AND date = '%s'
                    ''' % (table, stock, date)
            return pd.read_sql_query(query, self.conn)
        # 获取该股票当天的最新的两个分笔数据(获取最新的两个价格)
        else:
            query = '''SELECT * FROM '%s' 
                    WHERE stock_code = '%s' 
                    AND date = '%s'
                    ORDER BY time DESC
                    limit %d;''' % (table, stock, date, 2)
            return pd.read_sql_query(query, self.conn)



    def get_row_count(self, table, stock, date):
        query = '''SELECT count(*) from '%s' 
                WHERE date='%s' AND stock_code='%s';''' % (table, date, stock)
        self.c.execute(query)
        count = self.c.fetchone()[0]
        return count
    
    def get_tablename(self, freq):
        if freq == '1':
            return '1min'
        elif freq == '5':
            return '5min'
        elif freq == '15':
            return '15min'
        elif freq == '30':
            return '30min'
        elif freq == '60':
            return '60min'
        elif freq == 'D':
            return 'day'
        else:
            print("无效周期值")
            return None

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

if __name__ == '__main__':
    db = Database()
    # db.fetch('1min', '000333', 100)
    db.close()