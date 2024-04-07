#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_product_mapper.py"""

import sys

# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Разделяем строку по разделителю \t
    parts = line.strip().split('\t')
    if len(parts) == 2:  # Проверяем, что строка содержит две части
        product_id = parts[0].strip()
        name = parts[1].strip()

        print('{key}\tname\t{name}'.format(key=product_id, name=name))
