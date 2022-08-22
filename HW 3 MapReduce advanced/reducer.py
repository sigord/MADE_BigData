#!/usr/bin/env python3

import sys

def main():
    """Reducer"""

    cur_year = None
    cur_tag = None
    tag_count = 0

    for line in sys.stdin:
        year, tag, counts = line.split("\t")
        counts = int(counts)
        if year == cur_year and tag == cur_tag:
            tag_count += counts
        else:
            if cur_tag and cur_year:
                print(cur_year, cur_tag, tag_count, sep="\t")
            cur_year = year
            cur_tag = tag
            tag_count = counts
    if cur_tag and cur_year:
        print(cur_year, cur_tag, tag_count, sep="\t")

if __name__ == "__main__":
    main()
