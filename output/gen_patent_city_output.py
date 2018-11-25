from cooccurrence import co_patent_city
from cooccurrence import co_patent_class
import sqlite3
import networkx as nx
import csv

START_YEAR = 2000
END_YEAR = 2017
SPAN = 10
CITIES = ['TOKYO','SAN JOSE','CAMBRIDGE','TOYOTA','TROY','SHENZHEN','SHANGHAI','BEIJING']
GEN_ALL = True

K = 3
ALPHA = 0.5
BETA = 0.5

all_pat_cls_net = None
all_city_coop_net = None

if __name__ == '__main__':
    result = {}
    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')

    flag = 0
    for city in CITIES:
        print('正在计算{}的结果'.format(city.upper()))
        # 分别获取单个城市及全局的知识网络与合作网络
        if flag == 0:
            city_pat_cls_net, all_pat_cls_net = co_patent_class.get_each_range_network(con, START_YEAR, END_YEAR,
                                                                                       SPAN, city.upper(), GEN_ALL)
            city_coop_net, all_city_coop_net = co_patent_city.get_each_range_network(con, START_YEAR, END_YEAR, SPAN,
                                                                                     city.upper(), GEN_ALL)
            flag += 1
            GEN_ALL = False
        else:
            city_pat_cls_net = co_patent_class.get_each_range_network(con, START_YEAR, END_YEAR, SPAN, city.upper(),
                                                                      GEN_ALL)
            city_coop_net = co_patent_city.get_each_range_network(con, START_YEAR, END_YEAR, SPAN, city.upper(),
                                                                  GEN_ALL)

        # 计算K核相关内容
        ratio, avg_max_k_cc, outside_neighbour = co_patent_class.cal_k_core(city_pat_cls_net,all_pat_cls_net, SPAN, K)
        city_entropy = co_patent_class.cal_entropy(city_pat_cls_net, SPAN, ALPHA, BETA)
        all_entropy = co_patent_class.cal_entropy(all_pat_cls_net, SPAN, ALPHA, BETA)
        relative_entropy = {}
        for k, v in city_entropy.items():
            relative_entropy[k] = v / all_entropy[k]

        # 计算城市合作网络相关内容

        hhi = co_patent_city.cal_hhi(city_coop_net)
        page_rank = {}
        latitude = {}
        longitude = {}
        for each_year_net in all_city_coop_net:
            city_index = nx.get_node_attributes(each_year_net[1], 'CITY')
            tmp = {}
            for k, v in city_index.items():
                tmp[v.upper()] = k
            city_index = tmp

            attr = each_year_net[1].nodes[city_index[city.upper()]]
            page_rank[each_year_net[0]] = attr['Page Rank']
            latitude[each_year_net[0]] = attr['Latitude']
            longitude[each_year_net[0]] = attr['Longitude']

        # print('ratio:',ratio)
        # print('avg_max_k_cc',avg_max_k_cc)
        # print('neighbours:',outside_neighbour)
        # print('hhi',hhi)
        # print('PageRank',page_rank)
        # print('geocode-latitude', latitude)
        # print('geocode-longitude', longitude)

        result[city.upper()] = (ratio, avg_max_k_cc, outside_neighbour, hhi, page_rank, latitude, longitude)
        print('{}的结果计算完成'.format(city.upper()))

    for year in range(START_YEAR, END_YEAR - SPAN + 2):
        range_str = str(year) + '-' + str(year + SPAN - 1)
        print('正在输出{}的结果'.format(range_str))
        with open('../results/csv/{}-{}.csv'.format(str(year), str(year + SPAN - 1)), mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                ['city_name', 'ratio', 'avg_max_k_cc', 'neighbours', 'hhi', 'pagerank', 'latitude', 'longitude'])
            for city_name, values in result.items():
                writer.writerow(
                    [city_name, values[0][range_str], values[1][range_str], values[2][range_str], values[3][range_str],
                     values[4][range_str], values[5][range_str], values[6][range_str]])
