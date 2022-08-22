#!/usr/bin/env python3

import sys

TOP = 10

def main():
    """
        Filter which return top tags per year
    """
    cur_year = None
    counter = 0

    while True:
        line = sys.stdin.readline()
        if not line:
            break

        line = str(line)
        if len(line) > 4:
            year, tag, count, _ = line.split("\t")
            counter += 1
        else:
            continue
        if cur_year is None:
            cur_year = year
            print(year, tag, count, sep="\t", end="\n")
            continue
        if cur_year is not None and cur_year != year:
            print(year, tag, count, sep="\t", end="\n")
            counter = 1
            cur_year = year
            continue
        if cur_year == year and counter <= TOP:
            print(year, tag, count, sep="\t", end="\n")

if __name__ == "__main__":
    main()
