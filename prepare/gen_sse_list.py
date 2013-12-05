#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import urllib

url = 'http://www.sse.com.cn/js/common/ssesuggestdata.js'

stream = urllib.urlopen(url)
data = stream.read()
line_list = data.split('\n')
print len(line_list)
fout = open('sh.list', 'w')
for line in line_list:
    if line == '' or line[0] != '_':
        continue
    line = line[14:]
    if line[0] != '6':
        break
    fout.write(line[:6] + '\n')

fout.close()

