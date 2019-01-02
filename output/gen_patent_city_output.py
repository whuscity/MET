from cooccurrence import co_patent_city
from cooccurrence import co_patent_class
from geoUtil.forward_geocoding import get_geocode
import sqlite3
import networkx as nx
import csv
import os

START_YEAR = 2000
END_YEAR = 2016
SPAN = 5
# CITIES = ['TOKYO', 'SAN JOSE', 'CAMBRIDGE', 'TOYOTA', 'TROY', 'SHENZHEN', 'SHANGHAI', 'BEIJING']
# CITIES = []

# K = 10
ALPHA = 0.5
BETA = 0.5

global GEN_ALL
GEN_ALL = True

global FIRST_TIME
FIRST_TIME = True

global all_pat_cls_net
all_pat_cls_net = None

global all_city_coop_net
all_city_coop_net = None


def get_results(con, cities, extra_control_variables, production, K):
    global FIRST_TIME
    global all_pat_cls_net
    global all_city_coop_net
    global GEN_ALL

    FIRST_TIME = True
    result = {}

    print('正在计算K={}的系列结果'.format(K))
    for city in cities:
        print('正在计算{}的结果'.format(city.upper()))
        # 分别获取单个城市及全局的知识网络与合作网络
        if GEN_ALL:
            city_pat_cls_net, all_pat_cls_net = co_patent_class.get_each_range_network(con, START_YEAR, END_YEAR,
                                                                                       SPAN, city.upper(), GEN_ALL)
            city_coop_net, all_city_coop_net = co_patent_city.get_each_range_network(con, START_YEAR, END_YEAR, SPAN,
                                                                                     city.upper(), GEN_ALL)
            GEN_ALL = False
        else:
            city_pat_cls_net = co_patent_class.get_each_range_network(con, START_YEAR, END_YEAR, SPAN, city.upper(),
                                                                      GEN_ALL)
            city_coop_net = co_patent_city.get_each_range_network(con, START_YEAR, END_YEAR, SPAN, city.upper(),
                                                                  GEN_ALL)

        # 计算K核相关内容
        ratio, avg_max_k_cc, outside_neighbour, avg_dc, avg_triangle, avg_sh = co_patent_class.cal_k_core(
            city_pat_cls_net, all_pat_cls_net, SPAN, K)
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

        # 计算知识网络的基础指标
        ipc_density = co_patent_class.cal_density(city_pat_cls_net)

        # 计算城市合作网络相关内容（以及基本指标）

        hhi = co_patent_city.cal_hhi(city_coop_net)
        density = co_patent_city.cal_density(city_coop_net)
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

            # TODO: 把经纬度单独拿出来，防止城市不存在于合作网络导致999
            geocode = get_geocode([city.upper()])
            latitude[each_year_net[0]] = geocode[0][city.upper()]
            longitude[each_year_net[0]] = geocode[1][city.upper()]

            # try是防止城市不在全局网络中的出错
            try:
                # 根据节点ID获得属性
                attr = each_year_net[1].nodes[city_index[city.upper()]]
                page_rank[each_year_net[0]] = attr['Page Rank']
                # latitude[each_year_net[0]] = attr['Latitude']
                # longitude[each_year_net[0]] = attr['Longitude']

                avg_distance[each_year_net[0]] = attr['Average Distance']
                dc[each_year_net[0]] = attr['Normalized Degree Centrality']
                triangle[each_year_net[0]] = attr['Triangles']
                sh[each_year_net[0]] = attr['Structural Hole Constraint']
            except KeyError:
                page_rank[each_year_net[0]] = 0
                # latitude[each_year_net[0]] = 999
                # longitude[each_year_net[0]] = 999

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

        # result[city.upper()] = (city_node_num, ipc_node_num,
        #                         ratio, avg_max_k_cc, outside_neighbour, hhi, page_rank, latitude, longitude,
        #                         relative_entropy,
        #                         relative_entropy_node, relative_entropy_edge, avg_distance, dc, triangle, sh)
        result[city.upper()] = {'city_node_num': city_node_num,
                                'ipc_node_num': ipc_node_num,
                                'ratio': ratio,
                                'avg_max_k_cc': avg_max_k_cc,
                                'outside_neighbour': outside_neighbour,
                                'hhi': hhi,
                                'page_rank': page_rank,
                                'latitude': latitude,
                                'longitude': longitude,
                                'relative_entropy': relative_entropy,
                                'relative_entropy_node': relative_entropy_node,
                                'relative_entropy_edge': relative_entropy_edge,
                                'avg_distance': avg_distance,
                                'city_dc': dc,
                                'city_triangle': triangle,
                                'city_sh': sh,
                                'city_density': density,
                                'ipc_dc': avg_dc,
                                'ipc_triangle': avg_triangle,
                                'ipc_sh': avg_sh,
                                'ipc_density': ipc_density}

        print('{}的结果计算完成'.format(city.upper()))
        if len(result) >= 100:
            print('正在将缓存中的结果写入')
            for year in range(START_YEAR, END_YEAR - SPAN + 2):
                range_str = str(year) + '-' + str(year + SPAN - 1)
                print('正在输出{}的结果'.format(range_str))
                filename = '../results/csv/k_{}/{}-{}.csv'.format(K, str(year), str(year + SPAN - 1))
                os.makedirs(os.path.dirname(filename), exist_ok=True)

                with open(filename, mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if FIRST_TIME:
                        writer.writerow(
                            ['city_name',
                             'city_node_num',
                             'ipc_node_num',
                             'ratio',
                             'avg_max_k_cc',
                             'neighbours',
                             'hhi',
                             'pagerank',
                             'latitude',
                             'longitude',
                             'relative_entropy',
                             'relative_entropy_node',
                             'relative_entropy_edge',
                             'average_distance',
                             'city_degree_centrality',
                             'city_triangles',
                             'city_structural_hole_constraint',
                             'city_density',
                             'ipc_degree_centrality',
                             'ipc_triangles',
                             'ipc_structural_hole_constraint',
                             'ipc_density',
                             'knowledge_amount',
                             'inventor_amount',
                             'company_amount',
                             'production_inventor',
                             'production_origin',
                             'production_concat'])

                    for city_name, values in result.items():
                        # 三种产量
                        p_inventor = 0 if city_name not in production[range_str]['inventor'] else \
                            production[range_str]['inventor'][city_name]
                        p_origin = 0 if city_name not in production[range_str]['origin'] else \
                            production[range_str]['origin'][city_name]
                        p_concat = 0 if city_name not in production[range_str]['concat'] else \
                            production[range_str]['concat'][city_name]

                        # 额外的控制变量
                        knowledge_amount = 0 if city_name not in extra_control_variables[range_str]['knowledge_amount'] \
                            else extra_control_variables[range_str]['knowledge_amount'][city_name]
                        inventor_amount = 0 if city_name not in extra_control_variables[range_str]['inventor_amount'] \
                            else extra_control_variables[range_str]['inventor_amount'][city_name]
                        company_amount = 0 if city_name not in extra_control_variables[range_str]['company_amount'] \
                            else extra_control_variables[range_str]['company_amount'][city_name]

                        # try:
                        #     p_inventor = production[range_str]['inventor'][city_name]
                        #     p_origin = production[range_str]['origin'][city_name]
                        #     p_concat = production[range_str]['concat'][city_name]
                        #
                        # except KeyError:
                        #     p_inventor = 0
                        #     p_origin = 0
                        #     p_concat = 0
                        writer.writerow(
                            [city_name,
                             values['city_node_num'][range_str],
                             values['ipc_node_num'][range_str],
                             values['ratio'][range_str],
                             values['avg_max_k_cc'][range_str],
                             values['outside_neighbour'][range_str],
                             values['hhi'][range_str],
                             values['page_rank'][range_str],
                             values['latitude'][range_str],
                             values['longitude'][range_str],
                             values['relative_entropy'][range_str],
                             values['relative_entropy_node'][range_str],
                             values['relative_entropy_edge'][range_str],
                             values['avg_distance'][range_str],
                             values['city_dc'][range_str],
                             values['city_triangle'][range_str],
                             values['city_sh'][range_str],
                             values['city_density'][range_str],
                             values['ipc_dc'][range_str],
                             values['ipc_triangle'][range_str],
                             values['ipc_sh'][range_str],
                             values['ipc_density'][range_str],
                             knowledge_amount,
                             inventor_amount,
                             company_amount,
                             p_inventor,
                             p_origin,
                             p_concat])

            FIRST_TIME = False
            result.clear()
    if len(result) > 0:
        print('正在将剩余结果写入')
        for year in range(START_YEAR, END_YEAR - SPAN + 2):
            range_str = str(year) + '-' + str(year + SPAN - 1)
            print('正在输出{}的结果'.format(range_str))
            filename = '../results/csv/k_{}/{}-{}.csv'.format(K, str(year), str(year + SPAN - 1))
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            with open(filename, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                if FIRST_TIME:
                    writer.writerow(
                        ['city_name',
                         'city_node_num',
                         'ipc_node_num',
                         'ratio',
                         'avg_max_k_cc',
                         'neighbours',
                         'hhi',
                         'pagerank',
                         'latitude',
                         'longitude',
                         'relative_entropy',
                         'relative_entropy_node',
                         'relative_entropy_edge',
                         'average_distance',
                         'city_degree_centrality',
                         'city_triangles',
                         'city_structural_hole_constraint',
                         'city_density',
                         'ipc_degree_centrality',
                         'ipc_triangles',
                         'ipc_structural_hole_constraint',
                         'ipc_density',
                         'knowledge_amount',
                         'inventor_amount',
                         'company_amount',
                         'production_inventor',
                         'production_origin',
                         'production_concat'])

                for city_name, values in result.items():
                    # 三种产量
                    p_inventor = 0 if city_name not in production[range_str]['inventor'] else \
                        production[range_str]['inventor'][city_name]
                    p_origin = 0 if city_name not in production[range_str]['origin'] else \
                        production[range_str]['origin'][city_name]
                    p_concat = 0 if city_name not in production[range_str]['concat'] else \
                        production[range_str]['concat'][city_name]

                    # 额外的控制变量
                    knowledge_amount = 0 if city_name not in extra_control_variables[range_str]['knowledge_amount'] \
                        else extra_control_variables[range_str]['knowledge_amount'][city_name]
                    inventor_amount = 0 if city_name not in extra_control_variables[range_str]['inventor_amount'] \
                        else extra_control_variables[range_str]['inventor_amount'][city_name]
                    company_amount = 0 if city_name not in extra_control_variables[range_str]['company_amount'] \
                        else extra_control_variables[range_str]['company_amount'][city_name]

                    # try:
                    #     p_inventor = production[range_str]['inventor'][city_name]
                    #     p_origin = production[range_str]['origin'][city_name]
                    #     p_concat = production[range_str]['concat'][city_name]
                    #
                    # except KeyError:
                    #     p_inventor = 0
                    #     p_origin = 0
                    #     p_concat = 0
                    writer.writerow(
                        [city_name,
                         values['city_node_num'][range_str],
                         values['ipc_node_num'][range_str],
                         values['ratio'][range_str],
                         values['avg_max_k_cc'][range_str],
                         values['outside_neighbour'][range_str],
                         values['hhi'][range_str],
                         values['page_rank'][range_str],
                         values['latitude'][range_str],
                         values['longitude'][range_str],
                         values['relative_entropy'][range_str],
                         values['relative_entropy_node'][range_str],
                         values['relative_entropy_edge'][range_str],
                         values['avg_distance'][range_str],
                         values['city_dc'][range_str],
                         values['city_triangle'][range_str],
                         values['city_sh'][range_str],
                         values['city_density'][range_str],
                         values['ipc_dc'][range_str],
                         values['ipc_triangle'][range_str],
                         values['ipc_sh'][range_str],
                         values['ipc_density'][range_str],
                         knowledge_amount,
                         inventor_amount,
                         company_amount,
                         p_inventor,
                         p_origin,
                         p_concat])
        FIRST_TIME = False


def get_cities(con, limit):
    query_sql = 'SELECT UPPER(TRIM(city))||","||country FROM energy_inventor WHERE city IS NOT NULL AND city != "" GROUP BY city ORDER BY COUNT(*) DESC LIMIT {}'.format(
        limit)
    if limit <= 0:
        query_sql = 'SELECT UPPER(TRIM(city))||","||country FROM energy_inventor WHERE city IS NOT NULL AND city != "" GROUP BY city ORDER BY COUNT(*) DESC'

    cities = []
    cursor = con.cursor()
    cursor.execute(query_sql)
    results = cursor.fetchall()
    for row in results:
        cities.append(row[0])

    return cities


def get_each_year_production(con):
    cursor = con.cursor()
    result_dict = {}
    for year in range(START_YEAR, END_YEAR - SPAN + 2):
        query_inventor_production = 'SELECT `city`, COUNT(*) AS `num` ' \
                                    'FROM (SELECT `patnum`, `city`, `year` ' \
                                    'FROM (SELECT a.*, CAST(SUBSTR(b.`grantdate`,1,4) AS INTEGER) as `year` ' \
                                    'FROM `energy_inventor` as a LEFT JOIN `energy_conservation` as b ON a.`patnum` = b.`patnum`) ' \
                                    'WHERE year = ? AND `city` IS NOT NULL AND `city` != \'\'' \
                                    'GROUP BY `patnum`, `city`) ' \
                                    'WHERE `city` IS NOT NULL AND `city` != \'\'' \
                                    'GROUP BY UPPER(`city`) ' \
                                    'ORDER BY `num` DESC'

        query_origin_production = 'SELECT `city`, COUNT(*) AS num ' \
                                  'FROM energy_conservation WHERE CAST(SUBSTR(grantdate,1,4) AS INTEGER)=? ' \
                                  'AND `city` IS NOT NULL AND `city` != \'\' ' \
                                  'GROUP BY city ' \
                                  'ORDER BY num DESC'

        query_concat_production = 'SELECT city, COUNT(DISTINCT patnum) AS num ' \
                                  'FROM ( SELECT patnum, city, CAST(SUBSTR(grantdate,1,4) AS INTEGER) AS year ' \
                                  'FROM energy_conservation WHERE year=? ' \
                                  'UNION ' \
                                  'SELECT `patnum`, UPPER(`city`), `year` ' \
                                  'FROM (SELECT a.*, CAST(SUBSTR(b.`grantdate`,1,4) AS INTEGER) as `year` ' \
                                  'FROM `energy_inventor` as a ' \
                                  'LEFT JOIN ' \
                                  '`energy_conservation` as b ON a.`patnum` = b.`patnum`) ' \
                                  'WHERE year = ? AND `city` IS NOT NULL AND `city` != \'\' ' \
                                  'GROUP BY `patnum`, `city`) ' \
                                  'WHERE `city` IS NOT NULL AND `city` != \'\'' \
                                  'GROUP BY `city` ' \
                                  'ORDER BY num DESC'

        result_dict[str(year) + '-' + str(year + SPAN - 1)] = {}
        # 获取根据inventor表得到的产出
        cursor.execute(query_inventor_production, (year + SPAN,))
        results = cursor.fetchall()
        inner_dict = {}
        for row in results:
            inner_dict[row[0].upper()] = row[1]
        result_dict[str(year) + '-' + str(year + SPAN - 1)]['inventor'] = inner_dict

        # 获取根据原始表得到的产出
        cursor.execute(query_origin_production, (year + SPAN,))
        results = cursor.fetchall()
        inner_dict = {}
        for row in results:
            inner_dict[row[0].upper()] = row[1]
        result_dict[str(year) + '-' + str(year + SPAN - 1)]['origin'] = inner_dict

        # 整合两个表的去重产出
        cursor.execute(query_concat_production, (year + SPAN,year + SPAN))
        results = cursor.fetchall()
        inner_dict = {}
        for row in results:
            inner_dict[row[0].upper()] = row[1]
        result_dict[str(year) + '-' + str(year + SPAN - 1)]['concat'] = inner_dict

    return result_dict


def get_extra_control_variables(con):
    cursor = con.cursor()
    result_dict = {}
    for year in range(START_YEAR, END_YEAR - SPAN + 2):
        query_knowledge_amount = 'SELECT UPPER(energy_conservation.city), COUNT(DISTINCT patnum ) AS patent_num ' \
                                 'FROM energy_conservation ' \
                                 'WHERE CAST(SUBSTR( energy_conservation.grantdate, 1, 4 )AS INTEGER) BETWEEN ? AND ? ' \
                                 'AND `city` IS NOT NULL AND `city` != \'\'' \
                                 'GROUP BY energy_conservation.city ORDER BY patent_num DESC'

        query_inventor_amount = 'SELECT UPPER(a.city), COUNT(DISTINCT(a.name)) AS name_num ' \
                                'FROM (SELECT *, (first_name || last_name) AS name ' \
                                'FROM (select b.*, CAST(SUBSTR(c.`grantdate`,1,4) AS INTEGER) AS `year` ' \
                                'FROM energy_inventor AS b LEFT JOIN energy_conservation AS c ON b.patnum = c.patnum ' \
                                'WHERE YEAR BETWEEN ? AND ?) )AS a ' \
                                'WHERE `city` IS NOT NULL AND `city` != \'\'' \
                                'GROUP BY a.city ' \
                                'ORDER BY name_num DESC'

        query_company_amount = 'SELECT UPPER(city), COUNT(DISTINCT(owner)) AS owner_num ' \
                               'FROM energy_conservation ' \
                               'WHERE CAST(substr(grantdate, 1, 4 )AS INTEGER) BETWEEN ? AND ? ' \
                               'AND `city` IS NOT NULL AND `city` != \'\'' \
                               'GROUP BY city ORDER BY owner_num DESC'

        result_dict[str(year) + '-' + str(year + SPAN - 1)] = {}
        # 获取知识存量
        cursor.execute(query_knowledge_amount, (year, year + SPAN - 1))
        results = cursor.fetchall()
        inner_dict = {}
        for row in results:
            inner_dict[row[0].upper()] = row[1]
        result_dict[str(year) + '-' + str(year + SPAN - 1)]['knowledge_amount'] = inner_dict

        # 获取发明人数目
        cursor.execute(query_inventor_amount, (year, year + SPAN - 1))
        results = cursor.fetchall()
        inner_dict = {}
        for row in results:
            inner_dict[row[0].upper()] = row[1]
        result_dict[str(year) + '-' + str(year + SPAN - 1)]['inventor_amount'] = inner_dict

        # 获取公司数目
        cursor.execute(query_company_amount, (year, year + SPAN - 1))
        results = cursor.fetchall()
        inner_dict = {}
        for row in results:
            inner_dict[row[0].upper()] = row[1]
        result_dict[str(year) + '-' + str(year + SPAN - 1)]['company_amount'] = inner_dict

    return result_dict


def run():
    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    cities = get_cities(con, -1)
    production = get_each_year_production(con)
    extra_control_variables = get_extra_control_variables(con)
    for K in range(2, 11):
        get_results(con, cities, extra_control_variables, production, K)

    con.close()


if __name__ == '__main__':
    run()
