from netUtil.cooccurrence_network import get_cooccurrance_network_v2
from netUtil.add_network_properties import *
from netUtil.network_output import *
import sqlite3


def gen_co_city_network(cursor, start, end):
    """
    从energy_city_cooccurrence表中获取给定时间段的数据，
    并生成符合Cytoscape/Gephi规范的节点与边CSV文件

    :param cursor: 数据库游标
    :param start: 开始年份
    :param end: 结束年份
    :return: 该时间段的城市合作网络对象
    """
    query_sql = 'SELECT UPPER(TRIM(`city1`)), UPPER(TRIM(`city2`)) FROM `energy_city_cooccurrence` ' \
                'WHERE `year` BETWEEN ? AND ?'

    cursor.execute(query_sql, (start, end))
    results = cursor.fetchall()

    network = get_cooccurrance_network_v2(results)
    assert network is not None

    # 添加网络节点的基本属性
    # network = add_degree_centrality(network)
    # network = add_betweenness_centrality(network)
    # network = add_clustering_coefficient(network)
    # network = add_pagerank(network)
    # network = add_structural_holes_constraint(network)

    # 经纬度、国家为城市专属
    network = add_city_geocode(network)
    network = add_country(network)

    # 创建输出路径
    nodes_filename = '../results/energy/figure/{}-{}/{}-{}-city_nodes.csv'.format(start, end, start, end)
    edges_filename = '../results/energy/figure/{}-{}/{}-{}-city_edges.csv'.format(start, end, start, end)
    info_filename = '../results/energy/figure/{}-{}/{}-{}-city_info.csv' \
        .format(start, end, start, end)
    gexf_output_path = '../results/energy/figure/{}-{}/{}-{}-city.gexf'.format(start, end, start, end)

    # 用CSV格式输出网络
    csv_output(network, nodes_filename, edges_filename, info_filename)

    # gexf_output(network, gexf_output_path)

    return network


def run(con, start, end, span):
    cursor = con.cursor()

    # 生成各个时期的城市合作网络
    for year in range(start, end, span):
        print('正在生成 {}-{} 年的全局城市合作网络'.format(year, min(year + span - 1, end)))
        network = gen_co_city_network(cursor, year, min(year + span - 1, end))


if __name__ == '__main__':
    START_YEAR = 2000
    END_YEAR = 2017
    SPAN = 18

    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    run(con, START_YEAR, END_YEAR, SPAN)
    con.close()
