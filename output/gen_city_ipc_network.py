from netUtil.cooccurrence_network import get_cooccurrance_network_v2
from netUtil.add_network_properties import *
import sqlite3
import networkx as nx
from netUtil.network_output import csv_output


def gen_city_ipc_network(cursor, start, end):
    """
    从energy_ipc_city表中获取给定时间段的数据，
    并生成符合Cytoscape/Gephi规范的节点与边CSV文件

    :param cursor: 数据库游标
    :param start: 开始年份
    :param end: 结束年份
    :return: 该时间段的二分网络对象
    """
    query_sql = 'SELECT ipc, city||","||country AS city FROM "energy_ipc_city_full" WHERE city IS NOT NULL AND year BETWEEN ? AND ?'

    # 通过数据库连接查询所需的二分网络边结果
    cursor.execute(query_sql, (start, end))
    results = cursor.fetchall()

    # 调用函数创建二分网络，并检验其二分性
    network = get_cooccurrance_network_v2(results)
    assert nx.is_bipartite(network) == True

    # 为二分网络的不同类型节点做上标记（用0、1区分）
    network = add_node_type(network)

    # 添加网络节点的基本属性
    # network = add_degree_centrality(network)
    # network = add_betweenness_centrality(network)
    # network = add_clustering_coefficient(network)
    # network = add_pagerank(network)
    # network = add_structural_holes_constraint(network)

    # 知识复杂度为二分网络专属
    network = add_knowledge_complexity(network)

    # 创建输出路径
    nodes_filename = '../results/energy/figure/{}-{}/{}-{}-bipartite_nodes.csv'.format(start, end, start, end)
    edges_filename = '../results/energy/figure/{}-{}/{}-{}-bipartite_edges.csv'.format(start, end, start, end)
    info_filename = '../results/energy/figure/{}-{}/{}-{}-bipartite_info.csv' \
        .format(start, end, start, end)

    # 用CSV格式输出网络
    csv_output(network, nodes_filename, edges_filename, info_filename)

    return network


def run(con, start, end, span):
    cursor = con.cursor()

    # 生成各个时期的二分网络
    for year in range(start, end, span):
        print('正在生成 {}-{} 年的知识（IPC号）- 城市二分网络'.format(year, min(year + span - 1, end)))
        network = gen_city_ipc_network(cursor, year, min(year + span - 1, end))


if __name__ == '__main__':
    START_YEAR = 2000
    END_YEAR = 2017
    SPAN = 18

    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    run(con, START_YEAR, END_YEAR, SPAN)
    con.close()
