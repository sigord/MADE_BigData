#!/usr/bin/env python3
import sys
# import re
# from datetime import datetime
# import xml.etree.ElementTree as ET

DATE_KEY = 'CreationDate="'
TAG_KEY = 'Tags="'

def main():
    """
    Mapper
    - For more combiner efficiency increase NUM_ITERATIONS_PER_ITER constant
    """
    for line in sys.stdin:
        # get indexes of necessaries fields
        date_index = line.find(DATE_KEY)
        tag_index = line.find(TAG_KEY)
        # Check if necessary data exists
        if date_index != -1 and tag_index != -1:
            # find and parse date to get year
            start_date_index = date_index + len(DATE_KEY)
            end_date_index = line[start_date_index:].find("\"") + start_date_index
            # date = datetime.fromisoformat(line[start_date_index:end_date_index])
            # date = datetime.strptime(line[start_date_index:end_date_index], "%Y-%m-%dT%H:%M:%S.%f")
            year = line[start_date_index:end_date_index][:4]
            # find and parse tags
            start_tag_index = tag_index + len(TAG_KEY)
            end_tag_index = line[start_tag_index:].find("\"") + start_tag_index
            tags = line[start_tag_index:end_tag_index]
            tags = tags.replace('&lt;', '')
            tags = tags.replace('&gt;', ' ')
            tags = tags.split(' ')[:-1]
            if year in set(["2010", "2016"]):
                for tag in tags:
                    print(year, tag, 1, sep='\t')
          
if __name__ == "__main__":
    main()
    