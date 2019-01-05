import sqlite3
from geoUtil.forward_geocoding import get_geocode
import csv
from math import isclose
import os
from pyecharts import Style, Geo
import pandas as pd

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
    for year in range(START_YEAR, END_YEAR, SPAN):
        print('正在生成{}-{}年的知识-城市分布热力图'.format(year, min(year + SPAN - 1, END_YEAR)))
        result = gen_ipc_heatmap(con, year, min(year + SPAN - 1, END_YEAR))

        filename = '../results/energy/figure/{}-{}/{}-{}-heatmap.csv'\
            .format(year, min(year + SPAN - 1, END_YEAR),year, min(year + SPAN - 1, END_YEAR))
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, encoding='utf-8', mode='w', newline='') as file:
            writer = csv.writer(file, quoting=1)
            writer.writerow(['city_name', 'ipc_num', 'latitude', 'longitude'])
            for key, value in result.items():
                writer.writerow([key, value[0], value[1], value[2]])

        df = pd.read_csv(filename, delimiter=',')
        geocode = {df.iloc[i]['city_name']: [df.iloc[i]['longitude'], df.iloc[i]['latitude']] for i in
                   range(len(df))}
        attr = list(df['city_name'])
        value = list(df['ipc_num'])
        style = Style(title_color="#fff", title_pos="center", width=2400, height=1200, background_color="white")

        def symbolsize(params):
            return params[2]

        geo = Geo('各城市拥有IPC数量分布', **style.init_style)
        geo.add("", attr, value, visual_range=[0, 100], symbol='circle', symbol_size=symbolsize,
                visual_text_color="#fff", is_piecewise=True, type='scatter',
                is_visualmap=True, maptype='world', visual_split_number=10, geo_normal_color='#dcdcdc',
                geo_cities_coords=geocode)

        geo.render(filename[:-4] + '.html')

        print('生成完成\n')
    con.close()


