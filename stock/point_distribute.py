#!/usr/bin/env python
# -*- coding: utf-8 -*-
# point_distribute.py
import os
import pandas as pd
pathUrl = os.path.abspath('.') +'/stock/static/lxjm.txt'
def main():
    print('read txt. txtPath:{0}'.format(pathUrl))
    data = pd.read_table(pathUrl, header=None, encoding='UTF-8', delim_whitespace=True,index_col=None)
    print('read txt sucess.')
    print(data.iloc[2,2])
    # print(data)

if __name__ == '__main__':
    main()
