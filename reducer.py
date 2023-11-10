#!/usr/bin/env python3
import sys
from collections import defaultdict

car_data = defaultdict(lambda: {'count': 0, 'total_price': 0.0})

for line in sys.stdin:
    line = line.strip()
    geo_id, manufacturer, count_price = line.split('\t')
    
    count, price = map(float, count_price.split("^"))

    key = (geo_id, manufacturer)
    
    car_data[key]['count'] += count
    car_data[key]['total_price'] += price

for key, value in car_data.items():
    geo_id, manufacturer = key
    count = value['count']
    total_price = value['total_price']
    if count >= 10:
        output = "{}\t{}\t{}\t{}\n".format(geo_id, manufacturer, int(count), total_price)
        sys.stdout.write(output)
