#!/usr/bin/env python3
import sys
import random

for line in sys.stdin:
    line = str(random.randint(10000, 99999)) + '_' + line[:-1]
    print(line, None, sep="\t")
