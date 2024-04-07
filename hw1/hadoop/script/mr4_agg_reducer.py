#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr1_wc_reducer.py"""

import sys

# Константа
MIN_OCCURRENCES = 300

previous_key = None
previous_value = 0

# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Подготавливаем строку: удаляем пробелы в начале и конце
    prepared_line = line.strip()

    # Парсим строку: делим по первому \t
    key, value = prepared_line.split('\t', 1)
    value = int(value)

    # Т.к. все записи с одиннаковыми ключами попадут на один reducer и 
    # записи отсортированы по ключу на этапе shuffle, 
    # то можем подсчитать количество вхождений каждого слова
    if key == previous_key:
        previous_value += value
    else:
        # Сохраняем предыдущую
        if previous_key:
            if previous_value > MIN_OCCURRENCES:
                print('{key}-{value}'.format(key=previous_key, value=previous_value))
        
        # Накапливаем текущую
        previous_key = key
        previous_value = value
        

# После того, как строки закончились, не забываем записать результат для последнего ключа
if previous_key:
    if previous_value > MIN_OCCURRENCES:
        print('{key}-{value}'.format(key=previous_key, value=previous_value))