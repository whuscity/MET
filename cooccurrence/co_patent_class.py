import sqlite3
# import networkx as nx
from numpy import log
from netUtil.cooccurrence_network import *


def get_each_range_network(con, start, end, span, city='', gen_all=False):
    """
    生成各时间窗口的IPC共现网络

    :param start: 起始年份
    :param end: 结束年份
    :param span: 时间窗口长度
    :param city: 城市
    :return: networks 各时间窗口的IPC共现网络
    """
    city_networks = []
    all_networks = []
    cursor = con.cursor()

    for year in range(start, end - span + 2):
        # print('正在生成{}到{}年{}的IPC共现网络'.format(year, year + span - 1, city))
        city_query_sql = 'SELECT `ipc1`, `ipc2`, `year` FROM energy_ipc_cooccurrence WHERE `year` BETWEEN ? AND ? AND `city` LIKE ?'

        cursor.execute(city_query_sql, (year, year + span - 1, city.upper()))
        results = cursor.fetchall()

        city_patent_classes = generate_matrix_index(results)
        cur_city_network = get_cooccurrance_network(city_patent_classes, results, 'IPC')
        city_networks.append((str(year) + '-' + str(year + span - 1), cur_city_network))
        # nx.write_gexf(cur_city_network, '../results/' + str(year) + '-' + str(year + span - 1) + '-' + city + '.gexf')

        if gen_all:
            # print('正在生成{}到{}年的全局IPC共现网络'.format(year, year + span - 1))
            all_query_sql = 'SELECT `ipc1`, `ipc2`, `year` FROM energy_ipc_cooccurrence WHERE `year` BETWEEN ? AND ?'

            cursor.execute(all_query_sql, (year, year + span - 1))
            results = cursor.fetchall()

            all_patent_classes = generate_matrix_index(results)
            cur_all_network = get_cooccurrance_network(all_patent_classes, results, 'IPC')
            all_networks.append((str(year) + '-' + str(year + span - 1), cur_all_network))
            # nx.write_gexf(cur_all_network, '../results/' + str(year) + '-' + str(year + span - 1) + '-all.gexf')

    if gen_all:
        return city_networks, all_networks
    else:
        return city_networks


def find_max_k_core(network):
    """
    找到最高核子网

    :param network: networkX图对象
    :return pre: 最高核子网
    """
    k = 0
    pre = network
    while len(network.nodes) > 0:
        pre = network
        network = nx.k_core(network, k)
        k += 1
    return pre


# TODO:城市网络中没有与K核节点连通的部分怎么处理？
def cal_k_core(city_networks, all_networks, span, k=0):
    """
    计算同一城市各个年份的K核相关指标，每个指标的结果以单独的字典返回，键为年份跨度

    :param networks: 各时间窗口的IPC共现网络
    :param k: K核参数
    :return: 年份、K核节点占整体网络节点的比例、最高核节点在K核子网的平均接近中心度、K核子网与整体网络直接连接的节点数
    """
    # result = []
    ratio_dict = {}
    avg_max_k_cc_dict = {}
    outside_neighbors_dict = {}

    # 这里的处理比较麻烦，因为城市网络和全局网络是分开生成的，所以要通过城市网络
    # 建立对全局网络节点的映射。城市边的权重无法挽回。
    for city_network, all_network in zip(city_networks, all_networks):

        # 首先找到全局的K核子网，然后计算城市网络与全局网络K核子网的重叠部分占比

        city_net = city_network[1]
        all_net = all_network[1]
        knet = nx.k_core(all_net, k)


        # 重新建立联系
        city_labels = set(nx.get_node_attributes(city_net, 'IPC').values())
        city_nodes = set()
        all_labels = nx.get_node_attributes(all_net, 'IPC')
        for key, value in all_labels.items():
            if value in city_labels:
                city_nodes.add(key)
        city_net = all_net.subgraph(city_nodes)

        knet_nodes = set(knet.nodes)

        common = 0
        for node in knet_nodes:
            if node in city_nodes:
                common += 1


        if len(knet.nodes) <= 0:
            ratio = 0
        else:
            ratio = common / len(knet.nodes)

        # 计算城市网络中各个节点与最高K核全部节点的平均距离
        # 归一化参数N选择为最高K核全部节点的数量
        # 采用网络博客的方法来处理disconnected问题
        city_and_knet = all_net.subgraph(city_nodes.union(knet_nodes))
        max_knet = find_max_k_core(city_and_knet)
        max_knet_nodes = set(max_knet.nodes)
        all_node_sum = 0
        N = len(max_knet_nodes)

        for city_node in city_nodes:
            single_node_sum = 0
            for max_knet_node in max_knet_nodes:
                try:
                    single_node_sum += 1/(nx.shortest_path_length(city_and_knet, city_node, max_knet_node))
                except Exception as e:
                    single_node_sum += 0
            all_node_sum += single_node_sum / N
        if len(city_nodes) <= 0:
            avg_max_k_cc = 0
        else:
            avg_max_k_cc = all_node_sum / len(city_nodes)

        # 计算城市网络与全局网络的直接相邻节点数
        # TODO:这里会有蹭热点情况。
        outside_neighbors = set()
        for city_node in city_nodes:
            neighbors = all_net.neighbors(city_node)
            for neighbor in neighbors:
                if neighbor not in city_nodes:
                    outside_neighbors.add(neighbor)
        outside_neighbors_num = len(outside_neighbors)

        range_str = city_network[0]
        # result.append((str(network[0]) + '-' + str(network[0] + span - 1), ratio, avg_max_k_cc, outside_neighbors_num))
        ratio_dict[range_str] = ratio
        avg_max_k_cc_dict[range_str] = avg_max_k_cc
        outside_neighbors_dict[range_str] = outside_neighbors_num

        # print(sum(list(max_k_cc.values()))/len(max_k_cc), len(knet.nodes), len(max_k_core.nodes))
    return ratio_dict, avg_max_k_cc_dict, outside_neighbors_dict


def cal_entropy(networks, span, alpha=0.5, beta=0.5):
    entropys = {}
    # 对于给定的网络集合，遍历其中的每一个网络
    for network in networks:
        # 获取其中每一个节点的度数
        net: nx.Graph = network[1]
        degrees = net.degree
        degree_count = {}

        # 计算各个度数的次数
        for d in degrees:
            if d[1] in degree_count:
                degree_count[d[1]] += 1
            else:
                degree_count[d[1]] = 1
        # 计算不同度数出现的总次数（其实是等于节点数）
        degree_count_sum = sum(degree_count.values())

        # 计算不同度数的次数占比（分布）
        for k, v in degree_count.items():
            degree_count[k] = degree_count[k] / degree_count_sum

        # 论文公式4的计算，先计算出结构重要性（存入importance_list）
        importance_list = []
        N = len(net.nodes)
        for node_degree_pair in degrees:
            importance = alpha * (1 - degree_count[node_degree_pair[1]]) * N + beta * node_degree_pair[1] * (
                    1 - degree_count[node_degree_pair[1]]) * N
            importance_list.append(importance)

        # 对结构重要性求和得到总的结构重要性，逐一相除得到各个节点的相对重要性
        total_importance = sum(importance_list)

        #考虑结构熵为0的规则网络特殊情况
        if total_importance == 0:
            entropys[network[0]] = 0
            continue
        relative_importance_list = [imp / total_importance for imp in importance_list]

        # 利用相对重要性计算结构熵
        entropy = 0
        for relative_importance in relative_importance_list:
            entropy += -(relative_importance * log(relative_importance))

        entropys[network[0]] = entropy
    return entropys


def run():
    START_YEAR = 2000
    END_YEAR = 2017
    SPAN = 10
    CITY = 'SHANGHAI'

    con = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')

    print('==============生成网络====================')
    city_networks, all_networks = get_each_range_network(con, START_YEAR, END_YEAR, SPAN, CITY, gen_all=True)

    con.close()

    print('==============计算K核内容=================')
    ratio, avg_max_k_cc, outside_neighbour = cal_k_core(city_networks,all_networks, SPAN, 2)
    print('-----K核占比------')
    print(ratio)
    print('-----最高核CC------')
    print(avg_max_k_cc)
    print('-----直接相邻节点数------')
    print(outside_neighbour)

    print('==============计算熵=====================')
    city_entropy_result = cal_entropy(city_networks, SPAN, alpha=0.5, beta=0.5)

    print('-----城市------')
    print(city_entropy_result)
    all_entropy_result = cal_entropy(all_networks, SPAN, alpha=0.5, beta=0.5)

    print('-----全部------')
    print(all_entropy_result)

    print('-----熵比值-----')
    entropy_result = {}
    for k, v in city_entropy_result.items():
        entropy_result[k] = v / all_entropy_result[k]
    print(entropy_result)


if __name__ == '__main__':
    run()
