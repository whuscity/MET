import networkx as nx
from netUtil.cooccurrence_network import *
import sqlite3


def get_each_range_network(con, start, end, span, city, gen_all=False):
    city_networks = []
    all_networks = []
    cursor = con.cursor()

    for year in range(start, end - span + 2):
        print('正在生成{}到{}年{}的合作网络'.format(year, year + span - 1, city))
        city_query_sql = 'SELECT UPPER(TRIM(`city1`)), UPPER(TRIM(`city2`)), `year` FROM `energy_city_cooccurrence` ' \
                         'WHERE `year` BETWEEN {} AND {} AND (`city1` LIKE \'{}\' OR `city2` LIKE \'{}\')'.format(year,
                                                                                                                  year + span - 1,
                                                                                                                  city,
                                                                                                                  city)

        cursor.execute(city_query_sql)
        results = cursor.fetchall()

        cities = generate_matrix_index(results)
        cur_city_network = get_cooccurrance_network(cities, results, 'CITY')
        city_networks.append((str(year) + '-' + str(year + span - 1), cur_city_network))
        nx.write_gexf(cur_city_network, '../results/' + str(year) + '-' + str(year + span - 1) + '-' + city + '.gexf')

        if gen_all:
            print('正在生成{}到{}年的全部城市合作网络'.format(year, year + span - 1, city))
            all_query_sql = 'SELECT UPPER(TRIM(`city1`)), UPPER(TRIM(`city2`)), `year` FROM `energy_city_cooccurrence` ' \
                            'WHERE `year` BETWEEN {} AND {}'.format(year, year + span - 1)

            cursor.execute(all_query_sql)
            results = cursor.fetchall()

            all_cities = generate_matrix_index(results)
            cur_all_network = get_cooccurrance_network(all_cities, results, 'CITY')

            # 计算PageRank（边加权）,各个城市的PageRank加起来约等于1，附加到网络的节点属性中
            pagerank = nx.pagerank(cur_all_network, weight='weight')
            nx.set_node_attributes(cur_all_network, pagerank, 'Page Rank')

            all_networks.append((str(year) + '-' + str(year + span - 1), cur_all_network))
            nx.write_gexf(cur_all_network,
                          '../results/' + str(year) + '-' + str(year + span - 1) + '-all.gexf')

    if gen_all:
        return city_networks, all_networks
    else:
        return city_networks


def cal_hhi(networks):
    result_dict = {}
    for network in networks:
        hhi = 0

        net: nx.Graph = network[1]
        edge_weight = nx.get_edge_attributes(net, 'weight')
        total_weight = sum(edge_weight.values())
        for weight in edge_weight.values():
            hhi += (weight / total_weight) ** 2
        # print(hhi)
        result_dict[network[0]] = hhi
    return result_dict


if __name__ == '__main__':
    START_YEAR = 2000
    END_YEAR = 2017
    SPAN = 18

    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    city_networks, all_networks = get_each_range_network(con, START_YEAR, END_YEAR, SPAN, 'Yongin-si', gen_all=True)
    for i in cal_hhi(city_networks).items():
        print(i)
    con.close()
