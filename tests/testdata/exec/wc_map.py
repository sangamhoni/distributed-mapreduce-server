#!/usr/bin/env python3
"""Word count mapper."""
import sys


for line in sys.stdin:
    words = line.split()
    for word in words:
        print(f"{word}\t1")
    # assert False, "FIXME: for each word, print the word and a '1'"
