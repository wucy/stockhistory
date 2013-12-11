#!/usr/bin/env python

PAGESIZE = 700
WEB_TIME_OUT = 5
DM_TIME_OUT = 10
DATA_DIR = './data/air/'
DB_NAME = 'chn.db'

#API
TOT_PARAMS = 33


import urllib2
import time
import datetime
import threading
import os
import sqlite3

import Queue


INSERT_SQL = "INSERT OR IGNORE INTO `_TABLENAME_` (`id`, `stock_id`, `timestamp`, `open_price`, `yesterday_closing_price`, `now_price`, `high_price`, `low_price`, `now_buy_price`, `now_sell_price`, `volume`, `amount`, `buy_1_vol`, `buy_1_price`, `buy_2_vol`, `buy_2_price`, `buy_3_vol`, `buy_3_price`, `buy_4_vol`, `buy_4_price`, `buy_5_vol`, `buy_5_price`, `sell_1_vol`, `sell_1_price`, `sell_2_vol`, `sell_2_price`, `sell_3_vol`, `sell_3_price`, `sell_4_vol`, `sell_4_price`, `sell_5_vol`, `sell_5_price`) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS `%s` (`id` INTEGER PRIMARY KEY, `stock_id` TEXT, `timestamp` TEXT, `open_price` REAL, `yesterday_closing_price` REAL, `now_price` REAL, `high_price` REAL, `low_price` REAL, `now_buy_price` REAL, `now_sell_price` REAL, `volume` REAL, `amount` REAL, `buy_1_vol` REAL, `buy_1_price` REAL, `buy_2_vol` REAL, `buy_2_price` REAL, `buy_3_vol` REAL, `buy_3_price` REAL, `buy_4_vol` REAL, `buy_4_price` REAL, `buy_5_vol` REAL, `buy_5_price` REAL, `sell_1_vol` REAL, `sell_1_price` REAL, `sell_2_vol` REAL, `sell_2_price` REAL, `sell_3_vol` REAL, `sell_3_price` REAL, `sell_4_vol` REAL, `sell_4_price` REAL, `sell_5_vol` REAL, `sell_5_price` REAL, UNIQUE (`stock_id`, `timestamp`))"

def data_parser(data):
    """
    return a dict:
    key is a tuple (stock_id, date, time)
    value is a tuple contains parameters in the following order
    (
        open_price, yesterday_closing_price, 
        now_price, high_price, low_price, 
        now_buy_price, now_sell_price, #same as buy_1_price and sell_1_price
        volume, amount,
        buy_1_vol, buy_1_price,
        buy_2_vol, buy_2_price,
        buy_3_vol, buy_3_price,
        buy_4_vol, buy_4_price,
        buy_5_vol, buy_5_price,
        sell_1_vol, sell_1_price,
        sell_2_vol, sell_2_price,
        sell_3_vol, sell_3_price,
        sell_4_vol, sell_4_price,
        sell_5_vol, sell_5_price
    )
    """
    global TOT_PARAMS
    ret = dict()
    lines = data.split('\n')
    for line in lines:

        eq_pos = line.find('=')
        if eq_pos == -1:
            continue
        
        params_seg = line[eq_pos + 2:-1]
        params = params_seg.split(',')
        if len(params) != TOT_PARAMS:
            continue

        stock_id_seg = line[:eq_pos]
        stock_id = stock_id_seg[stock_id_seg.rfind('_') + 1:]
        date = params[30]
        time = params[31]
        #params[32] is nothing

        key = (stock_id, date, time)

        value = tuple(params[1:30])
        
        ret[key] = value
    return ret


def ensure_dir(file_name):
    root_dir = os.path.dirname(file_name)
    if root_dir == '':
        root_dir == '.'
    if not os.path.exists(root_dir):
        ensure_dir(root_dir)
        os.makedirs(root_dir)


def get_beijing_time():
    return datetime.datetime.utcnow() + datetime.timedelta(hours =+ 8)

class sqlite_db_manager(threading.Thread):
    def __init__(self, name, io_queue):
        threading.Thread.__init__(self)
        self.name = name
        self.io_queue = io_queue
        self.is_stop = False
        self.table_dict = set()
        
    def run(self):
        global DATA_DIR
        global DB_NAME
        global DM_TIME_OUT
        global CREATE_TABLE_SQL
        global INSERT_SQL

        file_name = DATA_DIR + '/' + DB_NAME
        ensure_dir(file_name)
        try:
            self.conn = sqlite3.connect(file_name)
            self.cursor = self.conn.cursor()
        except Exception, e:
            print e
            exit()
        while (not self.is_stop) or self.io_queue.qsize() != 0:
            if self.is_stop:
                print 'Waiting items in I/O queue:', self.io_queue.qsize(), time.ctime()
            current_beijing_date = get_beijing_time().strftime('%Y-%m-%d')
            table_name = 'table' + current_beijing_date.translate(None, '-')
            self.table_dict.add(table_name)
            init_table_sql = None
            try:
                init_table_sql = CREATE_TABLE_SQL % table_name
                x = self.cursor.execute(init_table_sql)
            except Exception, e:
                print e

            try:
                #print self.io_queue.qsize()
                data = self.io_queue.get(True, DM_TIME_OUT)
            except:
                print self.name, 'Data queue is empty. Still wait ...', time.ctime()
                continue
            try:
                post_data = []
                for item in data:
                    if item[1] != current_beijing_date:
                        continue
                    timestamp = ' '.join([item[1], item[2]])
                    content = [item[0], timestamp]
                    content.extend(list(data[item]))
                    post_data.append(tuple(content))
                insert = INSERT_SQL.replace('_TABLENAME_', table_name)
                self.cursor.executemany(insert, tuple(post_data))
                self.conn.commit()
            except Exception, e:
                print e
                print self.name, 'Error in data insertion to database!', time.ctime()
        self.cursor.close()
        self.conn.close()
        print self.name, 'is finished.'

    def stop(self):
        print 'Try to stop', self.name, '...'
        self.is_stop = True
    
    def monitor(self):
        print 'QUEUE_SIZE =', self.io_queue.qsize()
        print 'TOTAL_TABLE =', len(self.table_dict), '{',
        for name in self.table_dict:
            print name,
        print '}'

def is_trade_time():
    current_beijing_hms = get_beijing_time().strftime('%H:%M:%S')
    if current_beijing_hms < '08:40:00':
        return False
    if current_beijing_hms > '11:50:00' and current_beijing_hms < '12:40:00':
        return False
    if current_beijing_hms > '15:20:00':
        return False
    return True
 

class sub_crawler(threading.Thread):
    def __init__ (self, name, code_list, io_queue):
        threading.Thread.__init__(self)
        self.name = name
        self.code_list = code_list
        self.io_queue = io_queue
        self.is_stop = False
    def run(self):
        global WEB_TIME_OUT
        print self.name, 'starts!'
        code_join = ','.join(self.code_list)
        while not self.is_stop:
            if not is_trade_time():
                time.sleep(5)
                continue
            good = True
            content = ''
            try:
                #sta = time.time()
                content = urllib2.urlopen('http://hq.sinajs.cn/list=' + code_join, None, WEB_TIME_OUT).read()
                #end = time.time()
                #print end - sta
            except:
                print self.name, 'Network Timeout! Now try again ...', time.ctime()
                good = False
            if not good:
                continue
            data = data_parser(content)
            self.io_queue.put(data)
        print self.name, 'is finished.'
    def stop(self):
        print 'Try to stop', self.name, '...' 
        self.is_stop = True


def read_code(file_name, prefix):
    code_file = open(file_name)
    ret = []
    for code in code_file:
        code = code.strip()
        ret.append(prefix + code)
    return ret

def main():
    global PAGESIZE
    code_list = []
    code_list.extend(read_code('sz.list', 'sz'))
    code_list.extend(read_code('sh.list', 'sh'))
    print 'Get', len(code_list), 'stock id from lists'
   
    io_queue = Queue.Queue()

    db_task_name = 'db_manager'
    db_task = sqlite_db_manager(db_task_name, io_queue)
    db_task.setDaemon(True)
    db_task.start()

    task_list = []
    cnt = 1
    for start_id in range(0, len(code_list), PAGESIZE):
        end_id = min(start_id + PAGESIZE, len(code_list))
        sub_list = code_list[start_id : end_id]
        sub_task_name = 'sub_task' + str(cnt) + '[' + str(start_id) + ',' + str(end_id - 1) + ']'
        sub_task = sub_crawler(sub_task_name, sub_list, io_queue)
        sub_task.setDaemon(True)
        cnt += 1
        task_list.append(sub_task)
        sub_task.start()
    
    while True:
        signal=raw_input('Console# ').strip()
        if signal == 'exit':
            break
        elif signal == 'top':
            db_task.monitor()
        elif signal != '':
            print signal + ': command not found'

    for task in task_list:
        task.stop()
    for task in task_list:
        task.join()
    
    db_task.stop()
    db_task.join()


    print 'Crawler is finished!'





if __name__=='__main__':
    main()



