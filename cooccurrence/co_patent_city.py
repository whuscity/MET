import networkx as nx
from netUtil.cooccurrence_network import *
import sqlite3
from geoUtil.forward_geocoding import get_geocode
from math import isclose


def get_each_range_network(con, start, end, span, city, gen_all=False):
    """
    生成给定时间范围，以指定城市为中心的合作网络
    也可以生成该时间范围的全局合作网络

    :param con: 数据库连接
    :param start: 开始年份
    :param end: 结束年份
    :param span: 时间跨度
    :param city: 城市
    :param gen_all: 是否生成全局网络
    :return:
    """
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
        # nx.write_gexf(cur_city_network, '../results/' + str(year) + '-' + str(year + span - 1) + '-' + city + '.gexf')

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

            # 为城市节点添加地理编码
            city_name_dict = nx.get_node_attributes(cur_all_network, 'CITY')
            city_name = city_name_dict.values()
            latitude_dict, longitude_dict = get_geocode(city_name)

            latitude = {}
            longitude = {}
            for i, c in city_name_dict.items():
                #如果城市经纬度不存在，则删除该节点（因为这个其实不重要）
                if isclose(float(latitude[c]), float(999)):
                    cur_all_network.remove_node(i)
                    continue
                latitude[i] = latitude_dict[c]
                longitude[i] = longitude_dict[c]

            nx.set_node_attributes(cur_all_network, latitude, 'Latitude')
            nx.set_node_attributes(cur_all_network, longitude, 'Longitude')

            all_networks.append((str(year) + '-' + str(year + span - 1), cur_all_network))
            # nx.write_gexf(cur_all_network,
            #               '../results/' + str(year) + '-' + str(year + span - 1) + '-all.gexf')

    if gen_all:
        return city_networks, all_networks
    else:
        return city_networks


def cal_hhi(networks):
    """
    计算各个时间段城市的HHI，以字典形式返回，键为年份区间

    :param networks: 网络集合
    :return: HHI字典
    """
    result_dict = {}
    for network in networks:
        hhi = 0

        net: nx.Graph = network[1]
        edge_weight = nx.get_edge_attributes(net, 'weight')
        total_weight = sum(edge_weight.values())
        for weight in edge_weight.values():
            hhi += (weight / total_weight) ** 2
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
