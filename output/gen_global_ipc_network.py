from netUtil.cooccurrence_network import get_cooccurrance_network_v2
from netUtil.add_network_properties import *
from netUtil.network_output import csv_output
import sqlite3


def gen_co_ipc_network(cursor, start, end):
    """
    从energy_city_cooccurrence表中获取给定时间段的数据，
    并生成符合Cytoscape/Gephi规范的节点与边CSV文件

    :param cursor: 数据库游标
    :param start: 开始年份
    :param end: 结束年份
    :return: 该时间段的城市合作网络对象
    """
    query_sql = 'SELECT `ipc1`, `ipc2` FROM' \
                '( SELECT DISTINCT `patnum`, `ipc1`, `ipc2`, `year` FROM ' \
                'energy_ipc_cooccurrence WHERE `year` BETWEEN ? AND ?)'

    cursor.execute(query_sql, (start, end))
    results = cursor.fetchall()

    network = get_cooccurrance_network_v2(results)
    assert network is not None

    # 添加网络节点的基本属性
    network = add_degree_centrality(network)
    network = add_betweenness_centrality(network)
    network = add_clustering_coefficient(network)
    network = add_pagerank(network)
    # network = add_structural_holes_constraint(network)

    # 创建输出路径
    nodes_filename = '../results/energy/figure/{}-{}/{}-{}-ipc_nodes.csv'.format(start, end, start, end)
    edges_filename = '../results/energy/figure/{}-{}/{}-{}-ipc_edges.csv'.format(start, end, start, end)
    info_filename = '../results/energy/figure/{}-{}/{}-{}-ipc_info.csv' \
        .format(start, end, start, end)

    # 用CSV格式输出网络
    csv_output(network, nodes_filename, edges_filename, info_filename)

    return network


def run(con, start, end, span):
    cursor = con.cursor()

    # 生成各个时期的IPC共现网络
    for year in range(start, end, span):
        print('正在生成 {}-{} 年的全局IPC共现网络'.format(year, min(year + span - 1, end)))
        network = gen_co_ipc_network(cursor, year, min(year + span - 1, end))


if __name__ == '__main__':
    START_YEAR = 2000
    END_YEAR = 2017
    SPAN = 18

    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    run(con, START_YEAR, END_YEAR, SPAN)
    con.close()
