import csv
import networkx as nx
import os
from .add_network_properties import general_properties


def csv_output(network, nodes_output_path, edges_output_path, info_output_path, has_year=False):
    assert network is not None and nodes_output_path is not None and edges_output_path is not None and info_output_path is not None

    # 创建输出路径
    os.makedirs(os.path.dirname(nodes_output_path), exist_ok=True)
    os.makedirs(os.path.dirname(edges_output_path), exist_ok=True)
    os.makedirs(os.path.dirname(info_output_path), exist_ok=True)

    # 实际输出节点及其属性到CSV文件
    with open(nodes_output_path, encoding='utf-8', mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=';', quoting=0)

        # 获取属性名称以生成列名
        properties = list(network.node[list(network.nodes)[0]].keys())

        # 写入列名及属性值
        file.write('name;' + ';'.join(properties) + '\r\n')
        for node in network.nodes(data=True):
            output = [node[0]]
            for _, value in node[1].items():
                output.append(value)
            writer.writerow(output)

    # 实际输出边及其属性到CSV文件
    with open(edges_output_path, encoding='utf-8', mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=';', quoting=0)
        if not has_year:
            file.write('source;target;weight\r\n')
            weight = nx.get_edge_attributes(network, 'weight')
            for edge in network.edges:
                writer.writerow([edge[0], edge[1], weight[edge]])
        else:
            file.write('source;target;weight;year\r\n')
            weight = nx.get_edge_attributes(network, 'weight')
            year = nx.get_edge_attributes(network, 'year')
            for edge in network.edges:
                writer.writerow([edge[0], edge[1], weight[edge], year[edge]])

    # 将网络的整体指标输出
    with open(info_output_path, encoding='utf-8', mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=',', quoting=1)
        file.write('"property","value"\r\n')

        output_properties = general_properties(network)

        for key, value in output_properties.items():
            writer.writerow([key, value])


def gexf_output(network, output_path):
    assert network is not None and output_path is not None

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    nx.write_gexf(network, output_path)


def graphml_output(network, output_path):
    assert network is not None and output_path is not None

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    nx.write_graphml(network, output_path)