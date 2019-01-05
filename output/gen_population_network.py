from netUtil.cooccurrence_network import  get_cooccurrance_network_v2
from netUtil.add_network_properties import *
from netUtil.network_output import *
import sqlite3

def gen_pop_network(cursor, start, end):
    query_sql = 'SELECT b.city_eng AS source_city, c.city_eng AS target_city ' \
                'FROM province_network_final AS a LEFT JOIN province_city AS b ON a."outer" = b.pro ' \
                'LEFT JOIN province_city AS c ON a.inter = c.pro ' \
                'WHERE a.time BETWEEN ? AND ?'

    cursor.execute(query_sql, (start, end))
    results = cursor.fetchall()

    network = get_cooccurrance_network_v2(results, directed=True)
    assert network is not None

    # 创建输出路径
    nodes_filename = '../results/population/figure/{}-{}/{}-{}-city_nodes.csv'.format(start, end, start, end)
    edges_filename = '../results/population/figure/{}-{}/{}-{}-city_edges.csv'.format(start, end, start, end)
    info_filename = '../results/population/figure/{}-{}/{}-{}-city_info.csv' \
        .format(start, end, start, end)

    # 添加网络节点的基本属性
    network = add_degree_centrality(network)
    network = add_betweenness_centrality(network)
    network = add_closeness_centrality(network)
    network = add_clustering_coefficient(network)
    network = add_pagerank(network)
    network = add_structural_holes_constraint(network)

    # 经纬度、国家为城市专属
    network = add_city_geocode(network)
    network = add_country(network)
    network = add_average_geo_distance(network)

    # 添加出度和入读（有向图专属）
    network = add_in_and_out_degree(network)

    csv_output(network, nodes_filename, edges_filename, info_filename)

    return network

def run(con, start, end, span):
    cursor = con.cursor()

    # 生成各个时期的城市合作网络
    for year in range(start, end+1, span):
        print('正在生成 {}-{} 年的全局城市合作网络'.format(year, min(year + span - 1, end)))
        network = gen_pop_network(cursor, year, min(year + span - 1, end))


if __name__ == '__main__':
    START_YEAR = 2010
    END_YEAR = 2016
    SPAN = 1

    con = sqlite3.connect(r'C:\Users\Tom\Documents\population.db')
    run(con, START_YEAR, END_YEAR, SPAN)
    con.close()