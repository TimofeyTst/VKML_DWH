#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_mapper.py"""

import sys


# читаем строки из STDIN (standard input)
for line in sys.stdin:
    product_id, info_type, value = line.strip().split('\t')

    print('{key}\t{info_type}\t{value}'.format(key=product_id, info_type=info_type, value=value))


