#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr1_wc_mapper.py"""

import sys
import re

MIN_WORD_LENGTH = 3

# Функция для извлечения троек букв из слова
def extract_triples(word):
    if len(word) < MIN_WORD_LENGTH:
        return []
    triples = []
    for i in range(len(word) - 2):
        triples.append(word[i:i+3])
    return triples

def is_letter(char):
    return char.isalpha()

regex = re.compile('[^a-z ]+')

# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Подготавливаем строку: удаляем пробелы в начале и конце, 
    # приводим к нижнему регистру
    prepared_line = line.strip().lower()

    # Разбиваем строку на слова, игнорируя символы, цифры и пробелы
    words = re.split(r'[^a-z]+', prepared_line)
    
    for word in words:
        # Извлекаем тройки букв из слова
        triples = extract_triples(word)

        # Пишем в STDOUT (standard output) с разделителем \t
        for triple in triples:
            print('{key}\t{value}'.format(key=triple, value=1))