#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_price_mapper.py"""

import sys

# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Разделяем строку по разделителю ;
    parts = line.strip().split(';')
    if len(parts) == 2:  # Проверяем, что строка содержит две части
        product_id = parts[0].strip()
        price = parts[1].strip()

        print('{key}\tprice\t{price}'.format(key=product_id, price=price))
