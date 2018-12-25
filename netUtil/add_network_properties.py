import networkx as nx
from geoUtil.forward_geocoding import get_geocode
from networkx.algorithms import bipartite


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

# 集聚系数
def add_clustering_coefficient(network: nx.Graph):
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