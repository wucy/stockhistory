#!/usr/bin/env python

PAGESIZE = 70
WEB_TIME_OUT = 10
DATABASE_TIME_OUT = 10
TOT_PARAMS = 33
DATA_DIR = './data/stock_data/'



import urllib2
import time
import datetime
import threading
import shelve
import pickle
import os

import Queue




def ensure_dir(file_name):
    root_dir = os.path.dirname(file_name)
    if root_dir == '':
        root_dir == '.'
    if not os.path.exists(root_dir):
        ensure_dir(root_dir)
        os.makedirs(root_dir)


def data_parser(data, is_paramdict = False):
    """
    if not is_paramdict then
    return a dict:
    key is a tuple (stock_id, date, time)
    value is a tuple contains parameters in the following order

    if is_paramdict then
    return a dict:
    key is a tuple (stock_id, date, time)
    value is a dict {
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
    }
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
        
        value = None
        if not is_paramdict:
            value = tuple(params[1:30]);
        else:
            value = dict()
            #params[0] is stock name
            value['open_price'] = params[1]
            value['yesterday_closing_price'] = params[2]

            value['now_price'] = params[3]
            value['high_price'] = params[4]
            value['low_price'] = params[5]
        
            value['now_buy_price'] = params[6]
            value['now_sell_price'] = params[7]

            value['volume'] = params[8]
            value['amount'] = params[9]

            value['buy_1_vol'] = params[10]
            value['buy_1_price'] = params[11]
            value['buy_2_vol'] = params[12]
            value['buy_2_price'] = params[13]
            value['buy_3_vol'] = params[14]
            value['buy_3_price'] = params[15]
            value['buy_4_vol'] = params[16]
            value['buy_4_price'] = params[17]
            value['buy_5_vol'] = params[18]
            value['buy_5_price'] = params[19]

            value['sell_1_vol'] = params[20]
            value['sell_1_price'] = params[21]
            value['sell_2_vol'] = params[22]
            value['sell_2_price'] = params[23]
            value['sell_3_vol'] = params[24]
            value['sell_3_price'] = params[25]
            value['sell_4_vol'] = params[26]
            value['sell_4_price'] = params[27]
            value['sell_5_vol'] = params[28]
            value['sell_5_price'] = params[29]

        date = params[30]
        time = params[31]
        #params[32] is nothing
        key = (stock_id, date, time)

        ret[key] = value
    return ret


class db_manager(threading.Thread):
    def __init__(self, name, io_queue):
        threading.Thread.__init__(self)
        self.name = name
        self.io_queue = io_queue
        self.db_dict = dict()
        self.is_stop = False

    def run(self):
        global DATA_DIR
        global DATABASE_TIME_OUT
        while not self.is_stop:
            current_beijing_date = (datetime.datetime.utcnow() + datetime.timedelta(hours=+8)).strftime('%Y-%m-%d')
            db_name = os.path.join(DATA_DIR + '/', current_beijing_date + '.db')
            db = None
            if db_name not in self.db_dict:
                ensure_dir(db_name)
                db = shelve.open(db_name, 'c')
                self.db_dict[db_name] = db
            else:
                db = self.db_dict[db_name]
            try:
                data = self.io_queue.get(True, DATABASE_TIME_OUT)
            except:
                print self.name, 'Data queue is empty. Still wait ...'
                continue
            for item in data:
                #if item[1] != current_beijing_date:
                #    continue
                key = pickle.dumps((item[0], item[2]))
                db[key] = data[item]
        for name in self.db_dict:
            self.db_dict[name].close()
        print self.name, 'is finished.'

    def stop(self):
        print 'Try to stop', self.name, '...'
        self.is_stop = True
    
    def monitor(self):
        print 'TOTAL_DB =', len(self.db_dict)
        for key in self.db_dict:
            print 'NAME =', key, '\tSIZE =', len(self.db_dict[key])


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
            good = True
            content = ''
            try:
                content = urllib2.urlopen('http://hq.sinajs.cn/list=' + code_join, None, WEB_TIME_OUT).read()
            except:
                print self.name, 'Network Error! Now try again ...'
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
    db_task = db_manager(db_task_name, io_queue)
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

