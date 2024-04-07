#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_reducer.py"""

import sys

previous_key = None
products = []
prices = []

for line in sys.stdin:

    key, value = line.strip().split('\t', 1)
    value = value.split(':', 1)

    if key == previous_key:
        if value[0] == 'product':
            products.append(value[1])
        if value[0] == 'price':
            prices.append(value[1])
    else:
        if previous_key:
            for i in products:
                for j in prices:
                    print '{key}\t{product}\t{price}'.format(key=previous_key, product=i, price =j)
        previous_key = key
        products = []
        prices = []
        if value[0] == 'product':
            products.append(value[1])
        if value[0] == 'price':
            prices.append(value[1])


if previous_key:
    for i in products:
        for j in prices:
            print '{key}\t{product}\t{price}'.format(key=previous_key, product=i, price =j)
