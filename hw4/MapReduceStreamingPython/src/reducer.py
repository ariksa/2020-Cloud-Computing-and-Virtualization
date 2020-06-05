#!/usr/bin/env python3

import sys

word2count = dict()

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:
        continue

    # This is only to enable bash debugging.
    # In real hadoop we could use only 'count'
    word2count[word] = word2count.get(word, 0) + count

for word, total in word2count.items():
    print(word + "\t" + str(total))
