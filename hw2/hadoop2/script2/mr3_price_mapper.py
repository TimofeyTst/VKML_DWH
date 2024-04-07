#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_price_mapper.py"""

import sys

for line in sys.stdin:

    key, value = line.strip().split(';', 1)
    value = "price:" + value
    
    print '{key}\t{value}'.format(key=key, value=value)
