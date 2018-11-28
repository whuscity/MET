from cooccurrence import co_patent_city
from cooccurrence import co_patent_class
import sqlite3
import networkx as nx
import csv
import os

START_YEAR = 2000
END_YEAR = 2016
SPAN = 5
# CITIES = ['TOKYO', 'SAN JOSE', 'CAMBRIDGE', 'TOYOTA', 'TROY', 'SHENZHEN', 'SHANGHAI', 'BEIJING']
CITIES = []

# K = 10
ALPHA = 0.5
BETA = 0.5

global FIRST_TIME
FIRST_TIME = True

global all_pat_cls_net
all_pat_cls_net = None

global all_city_coop_net
all_city_coop_net = None


def get_results(con, production):
    global FIRST_TIME
    global all_pat_cls_net
    global all_city_coop_net
    result = {}
    GEN_ALL = True

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

        #网络只生成一次，更换K
        for K in range(2, 10):
            print('正在计算K={}的系列结果'.format(K))
            # 计算K核相关内容
            ratio, avg_max_k_cc, outside_neighbour = co_patent_class.cal_k_core(city_pat_cls_net, all_pat_cls_net, SPAN, K)
            city_entropy = co_patent_class.cal_entropy(city_pat_cls_net, SPAN, ALPHA, BETA)
            all_entropy = co_patent_class.cal_entropy(all_pat_cls_net, SPAN, ALPHA, BETA)
            relative_entropy = {}
            for k, v in city_entropy.items():
                relative_entropy[k] = v / all_entropy[k]

            city_entropy = co_patent_class.cal_entropy(city_pat_cls_net, SPAN, alpha=1, beta=0)
            all_entropy = co_patent_class.cal_entropy(all_pat_cls_net, SPAN, alpha=1, beta=0)
            relative_entropy_node = {}
            for k, v in city_entropy.items():
                relative_entropy_node[k] = v / all_entropy[k]

            city_entropy = co_patent_class.cal_entropy(city_pat_cls_net, SPAN, alpha=0, beta=1)
            all_entropy = co_patent_class.cal_entropy(all_pat_cls_net, SPAN, alpha=0, beta=1)
            relative_entropy_edge = {}
            for k, v in city_entropy.items():
                relative_entropy_edge[k] = v / all_entropy[k]

            # 计算城市合作网络相关内容（以及基本指标）

            hhi = co_patent_city.cal_hhi(city_coop_net)
            page_rank = {}
            latitude = {}
            longitude = {}
            avg_distance = {}
            dc = {}
            triangle = {}
            sh = {}
            city_node_num = {}
            ipc_node_num = {}
            for each_year_net in all_city_coop_net:
                # 找到城市名对应的节点ID
                city_index = nx.get_node_attributes(each_year_net[1], 'CITY')
                tmp = {}
                for k, v in city_index.items():
                    tmp[v.upper()] = k
                city_index = tmp

                # try是防止城市不在全局网络中的出错
                try:
                    # 根据节点ID获得属性
                    attr = each_year_net[1].nodes[city_index[city.upper()]]
                    page_rank[each_year_net[0]] = attr['Page Rank']
                    latitude[each_year_net[0]] = attr['Latitude']
                    longitude[each_year_net[0]] = attr['Longitude']

                    avg_distance[each_year_net[0]] = attr['Average Distance']
                    dc[each_year_net[0]] = attr['Normalized Degree Centrality']
                    triangle[each_year_net[0]] = attr['Triangles']
                    sh[each_year_net[0]] = attr['Structural Hole Constraint']
                except KeyError:
                    page_rank[each_year_net[0]] = 0
                    latitude[each_year_net[0]] = 999
                    longitude[each_year_net[0]] = 999

                    avg_distance[each_year_net[0]] = -1
                    dc[each_year_net[0]] = -1
                    triangle[each_year_net[0]] = -1
                    sh[each_year_net[0]] = -1

            for each_year_net2 in city_coop_net:
                city_node_num[each_year_net2[0]] = len(each_year_net2[1].nodes)

            for each_year_net3 in city_pat_cls_net:
                if len(each_year_net3[1].nodes) == 0:
                    ipc_node_num[each_year_net3[0]] = 1
                    continue
                else:
                    ipc_node_num[each_year_net3[0]] = len(each_year_net3[1].nodes)

            result[city.upper()] = (city_node_num, ipc_node_num,
                                    ratio, avg_max_k_cc, outside_neighbour, hhi, page_rank, latitude, longitude,
                                    relative_entropy,
                                    relative_entropy_node, relative_entropy_edge, avg_distance, dc, triangle, sh)
            print('{}的结果计算完成'.format(city.upper()))
            if len(result) >= 100:
                print('正在将缓存中的结果写入')
                for year in range(START_YEAR, END_YEAR - SPAN + 2):
                    range_str = str(year) + '-' + str(year + SPAN - 1)
                    print('正在输出{}的结果'.format(range_str))
                    filename = '../results/csv/k_{}/{}-{}.csv'.format(K, str(year), str(year + SPAN - 1))
                    os.makedirs(os.path.dirname(filename), exist_ok=True)

                    with open(filename, mode='a', newline='') as file:
                        writer = csv.writer(file)
                        if FIRST_TIME:
                            writer.writerow(
                                ['city_name', 'city_node_num', 'ipc_node_num', 'ratio', 'avg_max_k_cc', 'neighbours', 'hhi',
                                 'pagerank',
                                 'latitude', 'longitude',
                                 'relative_entropy', 'relative_entropy_node', 'relative_entropy_edge', 'average_distance',
                                 'degree_centrality', 'triangles', 'structural_hole_constraint', 'production'])

                        for city_name, values in result.items():
                            try:
                                p = production[range_str][city_name]
                            except KeyError:
                                p = 0
                            writer.writerow(
                                [city_name, values[0][range_str], values[1][range_str], values[2][range_str], values[3][range_str],
                                 values[4][range_str], values[5][range_str], values[6][range_str], values[7][range_str],
                                 values[8][range_str], values[9][range_str], values[10][range_str], values[11][range_str],
                                 values[12][range_str], values[13][range_str], values[14][range_str], values[15][range_str],p])
                FIRST_TIME = False
                result.clear()
            if len(result)>0:
                print('正在将剩余结果写入')
                for year in range(START_YEAR, END_YEAR - SPAN + 2):
                    range_str = str(year) + '-' + str(year + SPAN - 1)
                    print('正在输出{}的结果'.format(range_str))
                    filename = '../results/csv/k_{}/{}-{}.csv'.format(K, str(year), str(year + SPAN - 1))
                    os.makedirs(os.path.dirname(filename), exist_ok=True)

                    with open(filename, mode='a', newline='') as file:
                        writer = csv.writer(file)
                        if FIRST_TIME:
                            writer.writerow(
                                ['city_name', 'city_node_num', 'ipc_node_num', 'ratio', 'avg_max_k_cc', 'neighbours', 'hhi',
                                 'pagerank',
                                 'latitude', 'longitude',
                                 'relative_entropy', 'relative_entropy_node', 'relative_entropy_edge', 'average_distance',
                                 'degree_centrality', 'triangles', 'structural_hole_constraint', 'production'])

                        for city_name, values in result.items():
                            try:
                                p = production[range_str][city_name]
                            except KeyError:
                                p = 0
                            writer.writerow(
                                [city_name, values[0][range_str], values[1][range_str], values[2][range_str], values[3][range_str],
                                 values[4][range_str], values[5][range_str], values[6][range_str], values[7][range_str],
                                 values[8][range_str], values[9][range_str], values[10][range_str], values[11][range_str],
                                 values[12][range_str], values[13][range_str], values[14][range_str], values[15][range_str],p])
                FIRST_TIME = False

def get_cities(con, limit):
    query_sql = 'SELECT city FROM energy_inventor WHERE city IS NOT NULL AND city != \'\' GROUP BY city ORDER BY COUNT(*) DESC LIMIT {}'.format(
        limit)
    if limit <= 0:
        query_sql = 'SELECT city FROM energy_inventor WHERE city IS NOT NULL AND city != \'\' GROUP BY city ORDER BY COUNT(*) DESC'

    cursor = con.cursor()
    cursor.execute(query_sql)
    results = cursor.fetchall()
    for row in results:
        CITIES.append(row[0])


def get_each_year_production(con):
    cursor = con.cursor()
    result_dict = {}
    for year in range(START_YEAR, END_YEAR - SPAN + 2):
        query = 'SELECT `city`, COUNT(*) AS `num` ' \
                'FROM (SELECT `patnum`, `city`, `year` ' \
                'FROM (SELECT a.*, CAST(SUBSTR(b.`grantdate`,1,4) AS INTEGER) as `year` ' \
                'FROM `energy_inventor` as a LEFT JOIN `energy_conservation` as b ON a.`patnum` = b.`patnum`) ' \
                'WHERE year = ? AND `city` IS NOT NULL AND `city` != \'\'' \
                'GROUP BY `patnum`, `city`) GROUP BY UPPER(`city`) ORDER BY `num` DESC'

        cursor.execute(query, (year + SPAN,))
        results = cursor.fetchall()
        inner_dict = {}
        for row in results:
            inner_dict[row[0].upper()] = row[1]
        result_dict[str(year) + '-' + str(year + SPAN - 1)] = inner_dict

    # for i in result_dict.items():
    #     print(i)
    return result_dict


def run():
    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    get_cities(con, -1)
    production = get_each_year_production(con)
    get_results(con, production)

    con.close()


if __name__ == '__main__':
    run()
