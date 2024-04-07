#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_product_mapper.py"""

import sys

for line in sys.stdin:

    key, value = line.strip().split('\t', 1)
    value = "product:" + value
    
    print '{key}\t{value}'.format(key=key, value=value)
