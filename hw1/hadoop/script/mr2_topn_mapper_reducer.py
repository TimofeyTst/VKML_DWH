#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr2_topn_mapper_reducer.py"""

import sys
import re

N = 10

# В acc будем хранить N пар вида (key,value) с наибольшими value
acc = []
acc_has_n_element = False

def insert_in_sorted_acc(key, value):
    if value > acc[0][1]:
        index = 0
        for i in range(len(acc)):
            if value > acc[i][1]: 
                index = i+1
            else:
                break
        acc.insert(index, (key, value))
        acc.pop(0)


for line in sys.stdin:
    
    key, value = line.split('\t', 1)
    value = int(value)
    
    if acc_has_n_element:
        insert_in_sorted_acc(key, value)
    else:
        acc.append((key, value))
        if len(acc) == N:
            acc_has_n_element = True
            acc.sort(key=lambda x: x[1])
            
for key, value in acc:
    print '{key}\t{value}'.format(key=key, value=value)
