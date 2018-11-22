import networkx as nx
from netUtil.cooccurrence_network import *
import sqlite3


def get_each_range_network(con, start, end, span):
    city_networks = []
    cursor = con.cursor()

    for year in range(start, end - span + 2):
        print('正在生成{}到{}年的城市合作网络'.format(year, year + span - 1))
        query_sql = 'SELECT TRIM(`city1`), TRIM(`city2`), `year` FROM `energy_city_cooccurrence` ' \
                    'WHERE `year` BETWEEN {} AND {}'.format(year, year + span - 1)

        cursor.execute(query_sql)
        results = cursor.fetchall()

        cities = generate_matrix_index(results)
        cur_network = get_cooccurrance_network(cities, results, 'CITY')
        city_networks.append((str(year) + '-' + str(year + span - 1), cur_network))
        nx.write_gexf(cur_network, 'test.gexf')

    return city_networks

if __name__ == '__main__':
    START_YEAR = 2000
    END_YEAR = 2017
    SPAN = 18

    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    city_networks = get_each_range_network(con, START_YEAR, END_YEAR, SPAN)
    con.close()
