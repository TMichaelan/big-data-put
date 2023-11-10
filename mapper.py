#!/usr/bin/env python3

import sys

def mapper():
    for line in sys.stdin:
        fields = line.strip().split("^")
        
        if len(fields) == 18:
            geo_id = fields[17] 
            manufacturer = fields[2]
            price = fields[0]
            total_price = 0
            print(f"{geo_id}\t{manufacturer}\t{price}\t{total_price}")

if __name__ == "__main__":
    mapper()
