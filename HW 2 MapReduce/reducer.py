#!/usr/bin/env python3

import sys
import random

def main():
    """Reducer"""
    counter = None
    number_of_ids = None
    line = None
    iteration = 0

    while True:
        next_line = sys.stdin.readline()
        # Pass first iteration
        if iteration == 0:
            line = next_line
            iteration += 1
            continue
        # Check if the input line is last and print it
        if not next_line:
            intput_id, _ = line.split("\t")
            print(intput_id[6:], end='\n')
            break
        # Initialization of number of ids in output line
        if (number_of_ids is None) or (counter == number_of_ids):
            number_of_ids = random.randint(1, 5)
            counter = 0
        # If the output line length is greater than 1, print id with a comma at the end
        if counter < (number_of_ids - 1):
            intput_id, _ = line.split("\t")
            print(intput_id[6:], end=',')
            counter += 1
            line = next_line
            continue
        # if the output line length is 1 or it is last id in line print id with \n
        if counter == (number_of_ids - 1):
            intput_id, _ = line.split("\t")
            print(intput_id[6:], end='\n')
            counter += 1
            line = next_line
            continue

if __name__ == "__main__":
    main()
