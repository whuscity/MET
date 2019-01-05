from pandas import DataFrame
import networkx as nx

def generate_matrix_index(data):
    """

    :param data: 共现数据
    :return: 返回文件中不重复的类别名列表（按字母升序排列）
    """
    classes = set()

    for row in data:
        classes.add(str(row[0]).upper())
        classes.add(str(row[1]).upper())

    classes = list(classes)
    classes.sort()
    return classes


def get_cooccurrance_matrix(data, output_path=None):
    """
    计算并输出共现矩阵到CSV文件

    :param data: 共现数据
    :param output_path: 要输出共现矩阵的路径
    :return: df: 共现矩阵
    """
    classes = generate_matrix_index(data)
    co_matrix = [[0 for col in range(len(classes))] for row in range(len(classes))]
    classes_dict = {}
    for i in range(len(classes)):
        classes_dict[classes[i]] = i

    for row in data:
        co_matrix[classes_dict[row[0].upper()]][classes_dict[row[1].upper()]] += int(row[2])
        co_matrix[classes_dict[row[1].upper()]][classes_dict[row[0].upper()]] += int(row[2])

    df = DataFrame(co_matrix, columns=classes, index=classes)

    if output_path is not None:
        df.to_csv(output_path)

    return df


def get_cooccurrance_network(classes, data, label_name, directed=False):
    """
    计算并输出共现网络到GEXF文件

    :param classes: 前一步生成的类别名列表
    :param data: 共现数据
    :param output_path: 要输出共词网络图的路径
    :return: graph: 共现网络
    """
    classes_dict = {}
    reverse_classes_dict = {}
    for i in range(len(classes)):
        classes_dict[i + 1] = classes[i].upper()
        reverse_classes_dict[classes[i].upper()] = i + 1

    if not directed:
        graph = nx.Graph()
    else:
        graph = nx.DiGraph()
    graph.add_nodes_from(list(range(1, len(classes) + 1)))
    nx.set_node_attributes(graph, classes_dict, label_name)

    for row in data:
        if graph.has_edge(reverse_classes_dict[str(row[0]).upper()], reverse_classes_dict[str(row[1]).upper()]):
            graph.edges[reverse_classes_dict[str(row[0]).upper()], reverse_classes_dict[str(row[1]).upper()]]['weight'] += 1
        else:
            graph.add_edge(reverse_classes_dict[str(row[0]).upper()], reverse_classes_dict[str(row[1]).upper()], weight=1)


    # dc = nx.degree_centrality(graph)
    # bc = nx.betweenness_centrality(graph)
    # cc = nx.closeness_centrality(graph)
    # clustering = nx.triangles(graph)
    #
    # nx.set_node_attributes(graph, dc, 'Degree Centrality')
    # nx.set_node_attributes(graph, bc, 'Betweenness Centrality')
    # nx.set_node_attributes(graph, cc, 'Closeness Centrality')
    # nx.set_node_attributes(graph, clustering, 'Clustering Coefficient')

    return graph

def get_cooccurrance_network_v2(data, directed=False):
    """
    计算并输出共现网络到GEXF文件

    :param classes: 前一步生成的类别名列表
    :param data: 共现数据
    :param output_path: 要输出共词网络图的路径
    :return: graph: 共现网络
    """
    has_year = True

    if not directed:
        graph = nx.Graph()
    else:
        graph = nx.DiGraph()

    for row in data:
        if has_year:
            try:
                year = int(row[2])
            except Exception as e:
                print('不存在年份字段，使用普通方式建立网络')
                has_year = False

        if graph.has_edge(str(row[0]), str(row[1])):
            graph.edges[str(row[0]), str(row[1])]['weight'] += 1
        else:
            if has_year:
                graph.add_edge(str(row[0]), str(row[1]), weight=1, year=year)
            else:
                graph.add_edge(str(row[0]), str(row[1]), weight=1)
    return graph