#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr1_wc_mapper.py"""

import sys
import re

regex = re.compile('[^a-z ]+')

# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Подготавливаем строку: удаляем пробелы в начале и конце, 
    # приводим к нижнему регистру и удаляем все символы кроме букв и пробела
    prepared_line = regex.sub('', line.strip().lower())

    # Делим строку на слова по пробелам. 
    # В данном случае несколько пробелов считаются как один!
    words = prepared_line.split()
    
    for word in words:
        # Для каждого слова создаем пару (key, value):
        # key - слово
        # value - 1
        # Пишем в STDOUT (standard output) с разделителем \t
        print '{key}\t{value}'.format(key=word, value=1)