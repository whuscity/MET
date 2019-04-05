import networkx as nx
from geoUtil.forward_geocoding import get_geocode
from networkx.algorithms import bipartite
from netUtil.cooccurrence_network import *
from networkx.algorithms.bipartite import biadjacency_matrix
from networkx.algorithms import diameter, average_shortest_path_length
import numpy as np
import scipy.stats as ss
import re
from math import isclose
from networkx.algorithms import community
from geopy.distance import great_circle
from collections import defaultdict, Counter


# 点度中心度
def add_degree_centrality(network: nx.Graph):
    dc = nx.degree_centrality(network)
    nx.set_node_attributes(network, dc, 'Degree_Centrality')
    return network


# 中介中心度
def add_betweenness_centrality(network: nx.Graph):
    bc = nx.betweenness_centrality(network)
    nx.set_node_attributes(network, bc, 'Betweenness_Centrality')
    return network


def add_closeness_centrality(network: nx.Graph):
    cc = nx.closeness_centrality(network)
    nx.set_node_attributes(network, cc, 'Closeness_Centrality')
    return network


# 集聚系数
def add_clustering_coefficient(network: nx.Graph):
    if type(network) == type(nx.DiGraph()):
        print('正在将有向图转为无向图以计算集聚系数')
        tmp_net = network.to_undirected()
        cc = nx.clustering(tmp_net)
    else:
        cc = nx.clustering(network)
    nx.set_node_attributes(network, cc, 'Clustering_Coefficient')
    return network


# PageRank
def add_pagerank(network: nx.Graph):
    pr = nx.pagerank(network)
    nx.set_node_attributes(network, pr, 'Page_Rank')
    return network


# 结构洞约束
def add_structural_holes_constraint(network: nx.Graph):
    sh = nx.constraint(network)
    nx.set_node_attributes(network, sh, 'Structural_Holes_Constraint')
    return network


# 经纬度
def add_city_geocode(network: nx.Graph):
    cities = network.nodes
    latitude, longitude = get_geocode(cities)

    for city, lat in latitude.items():
        if isclose(lat, 999.0) or type(lat) != type(1.0):
            try:
                network.remove_node(city)
            except Exception as e:
                print(e)

    nx.set_node_attributes(network, latitude, 'Latitude')
    nx.set_node_attributes(network, longitude, 'Longitude')
    return network


# 国家
def add_country(network: nx.Graph):
    country_dict = {}
    for node in network.nodes:
        country_dict[node] = str(node)[-2:]

    nx.set_node_attributes(network, country_dict, 'Country')
    return network


# 全局参数
def general_properties(network: nx.Graph):
    properties = {
        'node_num': len(network.nodes),
        'edge_num': len(network.edges),
        'density': nx.density(network),
        'global_clustering_coefficient': nx.average_clustering(network)
        if type(network) == type(nx.Graph) else nx.average_clustering(network.to_undirected()),
        # 'diameter': diameter(network),
        'avg_shortest_path_length': average_shortest_path_length(network),
        'avg_degree': sum(dict(network.degree).values()) / len(network.nodes)
    }

    return properties


# 直接邻居数（在这里就是没有归一化的度数）
def add_neighbors(network: nx.Graph):
    nb = dict(network.degree)
    nx.set_node_attributes(network, nb, 'Neighbors(Unnormalized_Degree)')
    return network


# 区分二分网络的节点类型
def add_node_type(network: nx.Graph):
    assert nx.is_bipartite(network) == True

    node_type = bipartite.color(network)
    nx.set_node_attributes(network, node_type, 'Node_Type')
    return network


# 知识复杂度
def add_knowledge_complexity(network: nx.Graph, max_iteration=25):
    print('正在计算知识复杂度……')
    assert nx.is_bipartite(network)

    # 从知识-城市二分网络中取出知识和城市两类节点，并将其转换为列表
    set1, set2 = bipartite.sets(network)
    set1, set2 = list(set1), list(set2)

    # 通过判断列表中的第一个元素是否为IPC格式，进行知识和城市列表的确定
    pattern = re.compile(r'\w\d\d\w')
    if pattern.match(str(set1[0])) is not None:
        ipc_list = set1
        city_list = set2
    else:
        ipc_list = set2
        city_list = set1

    # 清空无用变量并对列表进行排序（以便后面重新绑定属性）
    set1 = None
    set2 = None
    ipc_list.sort()
    city_list.sort()

    # 使用networkx函数生成二分网络的邻接矩阵，其中行为城市，列为知识，加入权重
    matrix = biadjacency_matrix(network, city_list, ipc_list, weight='weight').toarray()

    # total为知识的加权总量
    total = matrix.sum()

    # kc0为每一行的和（按列求和），在这里表示每个城市产生知识的数量（城市的生产的diversity，多样性）
    kc0 = matrix.sum(1)
    # kp0为每一列的和（按行求和），在这里表示每个知识的产出城市数量（知识的ubiquity，普遍性）
    kp0 = matrix.sum(0)
    # product_share为每类知识在全球知识市场中所占的比值
    product_share = kp0 / total

    # matrix2是在原有邻接矩阵的基础上，根据RCA相对技术优势值是否大于等于1，转化成的01矩阵
    matrix2 = ((matrix / kc0.reshape(-1, 1)) / product_share.reshape(1, -1)) >= 1
    matrix2 = matrix2 * 1

    # 将最初值放入迭代结果列表
    kc = [kc0]
    kp = [kp0]
    max_i = 0

    for i in range(1, max_iteration):
        # 论文公式的向量化实现（使用矩阵相乘不需要循环）
        kci = np.matmul(matrix2, np.transpose(kp[i - 1])) / kc[0]
        kpi = np.matmul(np.transpose(kc[i - 1]), matrix2) / kp[0]

        # 检测排名是否与前2次发生变化，如果变化幅度小于某个设定值则跳出
        kci_rank = ss.rankdata(kci)
        kpi_rank = ss.rankdata(kpi)

        if i > 1:
            pre_kci_rank = ss.rankdata(kc[i - 2])
            if sum(kci_rank == pre_kci_rank) / len(kci_rank) >= 0.8:
                print('排名未发生太大变化，停止迭代，i=', i - 1)
                max_i = i - 1
                break

            pre_kpi_rank = ss.rankdata(kp[i - 2])
            if sum(kpi_rank == pre_kpi_rank) / len(kpi_rank) >= 0.8:
                print('排名未发生太大变化，停止迭代，i=', i - 1)
                max_i = i - 1
                break

        kc.append(kci)
        kp.append(kpi)

    for i in range(len(kc)):
        knowledge_complexity = {}
        kci_list = list(kc[i])
        kpi_list = list(kp[i])
        for city, kci in zip(city_list, kci_list):
            knowledge_complexity[city] = kci
        for ipc, kpi in zip(ipc_list, kpi_list):
            knowledge_complexity[ipc] = kpi

        nx.set_node_attributes(network, knowledge_complexity, 'Knowledge_Complexity_' + str(i))
        knowledge_complexity.clear()

    return network


# 社群发现（Girvan Newman算法）
def add_community_discovery(network: nx.Graph):
    q_value = []
    com = community.girvan_newman(network)
    for i in range(20):
        print('正在计算K={}的社群'.format(i + 2))
        cur_com = next(com)
        q_value.append((len(cur_com), community.modularity(network, cur_com), cur_com))

    max_q = max(q_value, key=lambda x: x[1])
    print('模块度最大的社群数为：{}，模块度为：{}'.format(max_q[0], max_q[1]))

    group_dict = {}
    group_id = 1
    for group in max_q[2]:
        for node in group:
            group_dict[node] = group_id
        group_id += 1

    nx.set_node_attributes(network, group_dict, 'Community')

    return network, q_value


# 流入/流出（出度和入度）
def add_in_and_out_degree(network: nx.DiGraph):
    assert type(network) == type(nx.DiGraph())

    in_degree = dict(network.in_degree(weight='weight'))
    out_degree = dict(network.out_degree(weight='weight'))

    nx.set_node_attributes(network, in_degree, 'In_Degree')
    nx.set_node_attributes(network, out_degree, 'Out_Degree')

    return network


# 计算地理上的到其他节点的平均距离（不需要有网络连通关系）
def add_average_geo_distance(network: nx.Graph):
    avg_geo_distance = {}
    for city1 in network.nodes(data=True):
        city1_name = city1[0]
        city1_geocode = city1[1]['Latitude'], city1[1]['Longitude']
        single_city_distance_sum = 0
        for city2 in network.nodes(data=True):
            if city1 == city2:
                continue
            else:
                city2_geocode = city2[1]['Latitude'], city2[1]['Longitude']
                single_city_distance_sum += great_circle(city1_geocode, city2_geocode).kilometers
        avg_geo_distance[city1_name] = single_city_distance_sum / (len(network.nodes) - 1)

    nx.set_node_attributes(network, avg_geo_distance, 'Average_Geo_Distance')
    return network


# 计算基于Search Path Count的引文网络路径权重
def add_search_path_count_weight(network: nx.DiGraph):
    assert nx.is_directed_acyclic_graph(network)

    # 找到所有的源点（sources）和汇点（sinks）
    sources = [k for k, v in dict(network.in_degree).items() if v == 0]
    sinks = [k for k, v in dict(network.out_degree).items() if v == 0]

    # 权重字典，初始权重设置为0
    spc_weight_dict = defaultdict(lambda: 0)
    paths = []
    global_main_paths = []

    # 收集所有从源点到汇点的路径
    for source in sources:
        for sink in sinks:
            paths += nx.algorithms.all_simple_paths(network, source, sink)

    # 计算每条边被走过的次数并存放
    for path in paths:
        for i in range(len(path) - 1):
            spc_weight_dict[(path[i], path[i + 1])] += 1

    # 使用全局搜索方法找到主路径
    max_weight = -1
    for path in paths:
        cur_weight = 0
        for i in range(len(path) - 1):
            cur_weight += spc_weight_dict[(path[i], path[i + 1])]
        if cur_weight > max_weight:
            max_weight = cur_weight
            global_main_paths.clear()
            global_main_paths.append(path)
        elif cur_weight == max_weight:
            # 可能有多条权重相等的主路径，一并提取
            global_main_paths.append(path)

    nx.set_edge_attributes(network, spc_weight_dict, 'Search Path Count')
    return network, global_main_paths, max_weight


def gen_meme_path_properties_dict(network: nx.DiGraph):
    assert type(network) == type(nx.DiGraph())

    node_num = len(network.nodes)
    edge_num = len(network.edges)

    in_degree_list = [in_degree[1] for in_degree in network.in_degree]
    max_in_degree = max(in_degree_list)
    min_in_degree = min(in_degree_list)
    avg_in_degree = sum(in_degree_list) / len(in_degree_list)

    assert max_in_degree >= min_in_degree and max_in_degree >= avg_in_degree

    out_degree_list = [out_degree[1] for out_degree in network.out_degree]
    max_out_degree = max(out_degree_list)
    min_out_degree = min(out_degree_list)
    avg_out_degree = sum(out_degree_list) / len(out_degree_list)

    assert max_out_degree >= min_out_degree and max_out_degree >= avg_out_degree

    density = nx.density(network)

    num_components = nx.number_connected_components(network.to_undirected())

    # 采用不是最优的算法来算直径
    max_component = network.subgraph(max(nx.connected_components(network.to_undirected()), key=len))
    max_component_diameter = nx.diameter(max_component.to_undirected())

    if num_components > 1:
        max_component_node_num = len(max_component.nodes)
        max_component_edge_num = len(max_component.edges)

        mc_in_degree_list = [mc_in_degree[1] for mc_in_degree in max_component.in_degree]
        max_component_max_in_degree = max(mc_in_degree_list)
        max_component_min_in_degree = min(mc_in_degree_list)
        max_component_avg_in_degree = sum(mc_in_degree_list) / len(mc_in_degree_list)

        mc_out_degree_list = [mc_out_degree[1] for mc_out_degree in max_component.out_degree]
        max_component_max_out_degree = max(mc_out_degree_list)
        max_component_min_out_degree = min(mc_out_degree_list)
        max_component_avg_out_degree = sum(mc_out_degree_list) / len(mc_out_degree_list)

    else:
        max_component_node_num = node_num
        max_component_edge_num = edge_num

        max_component_max_in_degree = max_in_degree
        max_component_min_in_degree = min_in_degree
        max_component_avg_in_degree = avg_in_degree
        max_component_max_out_degree = max_out_degree
        max_component_min_out_degree = min_out_degree
        max_component_avg_out_degree = avg_out_degree

    max_component_in_degree_distribution = Counter([i[1] for i in max_component.in_degree])
    max_component_out_degree_distribution = Counter([i[1] for i in max_component.out_degree])

    return {'cascade_size': max_component_node_num,
            'max_comp_edge_count': max_component_edge_num,
            'cascade_depth': max_component_diameter,
            'component_num': num_components,
            'in_degree': sum([i[1] for i in network.in_degree]),
            'out_degree': sum([i[1] for i in network.out_degree]),
            'degree': sum([i[1] for i in network.degree]),
            'node_num': len(network.nodes),
            'edge_count': len(network.edges),
            'clustering_coef': nx.average_clustering(network.to_undirected()),
            'density': nx.density(network.to_undirected()),
            'source_num': Counter([i[1] for i in network.in_degree])[0] #找到入度为0的起源节点数
            }

    # return {'node_num': node_num,
    #         'edge_num': edge_num,
    #         'max_in_degree': max_in_degree,
    #         'max_out_degree': max_out_degree,
    #         'avg_in_degree': avg_in_degree,
    #         'avg_out_degree': avg_out_degree,
    #         'min_in_degree': min_in_degree,
    #         'min_out_degree': min_out_degree,
    #         'density': density,
    #         'num_components': num_components,
    #         'max_component_node_num':max_component_node_num,
    #         'max_component_edge_num' : max_component_edge_num,
    #         'max_component_diameter': max_component_diameter,
    #         'max_component_max_in_degree': max_component_max_in_degree,
    #         'max_component_min_in_degree': max_component_min_in_degree,
    #         'max_component_avg_in_degree': max_component_avg_in_degree,
    #         'max_component_max_out_degree': max_component_max_out_degree,
    #         'max_component_min_out_degree': max_component_min_out_degree,
    #         'max_component_avg_out_degree': max_component_avg_out_degree}
