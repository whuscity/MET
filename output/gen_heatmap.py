import sqlite3
from geoUtil.forward_geocoding import get_geocode
import csv
from math import isclose

def gen_ipc_heatmap(con, start, end):
    query_sql = 'SELECT city, count( * ) FROM ' \
                '( SELECT city, ipc FROM energy_ipc_city ' \
                'WHERE city IS NOT NULL AND `year` BETWEEN ? AND ?' \
                'GROUP BY city, ipc ) AS a ' \
                'GROUP BY city ORDER BY count( * ) DESC'

    cursor = con.cursor()
    cursor.execute(query_sql, (start,end))
    results = cursor.fetchall()

    result_dict = {}

    for row in results:
        city = str(row[0])
        num = int(row[1])
        try:
            geocode = get_geocode([city])
            latitude, longitude = list(geocode[0].values())[0], list(geocode[1].values())[0]
            if isclose(latitude, 999.0):
                continue

            result_dict[city] = (num, latitude, longitude)
        except Exception as e:
            print(e)

    return result_dict

if __name__ == '__main__':
    START_YEAR = 2000
    END_YEAR = 2017
    SPAN = 5

    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    result = gen_ipc_heatmap(con, START_YEAR, END_YEAR)
    con.close()

    with open('../results/energy/figure/heatmap.csv', encoding='utf-8', mode='w', newline='') as file:
        writer = csv.writer(file, quoting=1)
        writer.writerow(['city_name', 'ipc_num', 'latitude', 'longitude'])
        for key,value in result.items():
            writer.writerow([key[-2:], value[0], value[1], value[2]])
