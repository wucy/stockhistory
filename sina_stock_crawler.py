#!/usr/bin/env python

PAGESIZE = 70
TIME_OUT = 10
TOT_PARAMS = 33
DATA_DIR = './stock_data/'

import urllib2
import time
import threading
import shelve

#return a map, key is stock_id, value is a tuple (first_entry is time, second is a tuple of other infos)
def data_parser(data):
    global TOT_PARAMS
    ret = dict()
    lines = data.split('\n')
    for line in lines:
        eq_pos = line.find('=')
        if eq_pos == -1:
            continue
        
        raw_key = line[:eq_pos]
        key = raw_key[raw_key.rfind('_') + 1:]

        raw_params = line[eq_pos + 2:-2]
        params = raw_params.split(',')
        
        if len(params) != 33:
            continue
        timestamp = time.mktime(time.strptime('T'.join(params[-3:-1]), "%Y-%m-%dT%H:%M:%S"))
        

        value = (int(timestamp), tuple(params[2:-3]))
        ret[key] = value
    return ret

class sub_crawler(threading.Thread):

    def __init__ (self, name, code_list):
        threading.Thread.__init__(self)
        self.name = name
        self.code_list = code_list
        self.is_stop = False

    def run(self):
        global TIME_OUT
        global DATA_DIR
        print self.name, 'starts!'
        code_join = ','.join(self.code_list)
        
        db_dict = dict()
        for code in self.code_list:
            db_dict[code] = shelve.open(DATA_DIR + '/' + code + '.db', 'c')

        while not self.is_stop:
            good = True
            content = ''
            try:
                content = urllib2.urlopen('http://hq.sinajs.cn/list=' + code_join, None, TIME_OUT).read()
            except:
                print self.name, 'Network Error! Now try again...'
                good = False
            if not good:
                continue
            
            data = data_parser(content)
            for key in data:
                db = db_dict[key]
                timestamp = data[key][0]
                value = data[key][1]
                db[str(timestamp)] = value

        for code in self.code_list:
            db_dict[code].close()

        print self.name, 'is finished.'

    def stop(self):
        print 'try to stop', self.name, '...' 
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
    #code_list.extend(read_code('sz.list', 'sz'))
    code_list.extend(read_code('sh.list', 'sh'))
    print len(code_list)

    task_list = []
    cnt = 1
    for start_id in range(0, len(code_list), PAGESIZE):
        end_id = min(start_id + PAGESIZE, len(code_list))
        sub_list = code_list[start_id : end_id]
        sub_task_name = 'sub_task' + str(cnt) + '[' + str(start_id) + ',' + str(end_id - 1) + ']'
        sub_task = sub_crawler(sub_task_name, sub_list)
        sub_task.setDaemon(True)
        cnt += 1
        task_list.append(sub_task)
        sub_task.start()

    while True:
        signal=raw_input('Console# ')
        if signal.strip() == 'exit':
            for task in task_list:
                task.stop()
            break
    for task in task_list:
        task.join()

    print 'Crawler is finished!'





if __name__=='__main__':
    main()

