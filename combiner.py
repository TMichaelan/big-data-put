#!/usr/bin/env python3

import sys

def combiner():
    current_geo_id = None
    current_manufacturer = None
    car_count = 0
    total_price = 0

    for line in sys.stdin:
        geo_id, manufacturer, price, tmp = line.strip().split("\t")

        try:
            price = float(price)
        except ValueError:
            continue

        if current_geo_id != geo_id or current_manufacturer != manufacturer:
            if current_geo_id:
                print(f"{current_geo_id}\t{current_manufacturer}\t{car_count}^{total_price}")

            current_geo_id = geo_id
            current_manufacturer = manufacturer
            car_count = 1
            total_price = price
        else:
            car_count += 1
            total_price += price

    if current_geo_id:
        print(f"{current_geo_id}\t{current_manufacturer}\t{car_count}^{total_price}")

if __name__ == "__main__":
    combiner()
