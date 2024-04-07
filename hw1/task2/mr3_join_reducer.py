#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_reducer.py"""

import sys

previous_id = None
previous_type = None
previous_value = None

# Читаем входные данные из потока ввода
for line in sys.stdin:
    product_id, info_type, value = line.strip().split('\t')
    
    # Если подряд одинаковый тип информации, пропускаем
    if previous_id == product_id and previous_type != info_type:
        if info_type == 'price':
            name = previous_value
            price = float(value) # Можно впринципе оставить строку
        else:
            name = value
            price = float(previous_value) # Можно впринципе оставить строку

        previous_id = None
        previous_info_type = None
        print('{key}\t{name}\t{price}'.format(key=product_id, name=name, price=price))
    else:
        previous_id = product_id
        previous_type = info_type
        previous_value = value
