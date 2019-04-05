from DBUtil import get_db_connection
from netUtil.cooccurrence_network import generate_matrix_index, get_cooccurrance_network, get_cooccurrance_network_v2
from netUtil.add_network_properties import gen_meme_path_properties_dict
import networkx as nx
import csv
from collections import Counter


def gen_meme_path(con):
    query_sql = 'SELECT UPPER(CONCAT_WS(":", paper_id, "HELATH")), UPPER(CONCAT_WS(":", cited_paper_id, cited_paper_db)), ' \
                'citing_paper_year, cited_paper_year, cited_paper_db ' \
                'FROM health_meme_path'

    cursor = con.cursor()
    cursor.execute(query_sql)
    results = cursor.fetchall()

    nodes = generate_matrix_index(results)
    network = get_cooccurrance_network(nodes, results, 'PAPER_ID', directed=True)

    _paper_id_dict = nx.get_node_attributes(network, 'PAPER_ID')
    paper_id_dict = {}
    for k,v in _paper_id_dict.items():
        paper_id_dict[v] = k

    # print(paper_id_dict)

    year_dict = {}
    db_dict = {}
    for row in results:
        paper_id = str(row[0])
        cited_paper_id = str(row[1])
        citing_paper_year = row[2]
        cited_paper_year = row[3]
        cited_paper_db = row[4]

        if paper_id in year_dict and cited_paper_id in year_dict:
            pass
        else:
            year_dict[paper_id_dict[paper_id]] = citing_paper_year
            year_dict[paper_id_dict[cited_paper_id]] = cited_paper_year

        if paper_id in db_dict and cited_paper_id in db_dict:
            pass
        else:
            db_dict[paper_id_dict[paper_id]] = 'health'
            db_dict[paper_id_dict[cited_paper_id]] = cited_paper_db



    nx.set_node_attributes(network, year_dict, 'YEAR')
    nx.set_node_attributes(network, db_dict, 'CITED_DB')
    nx.write_gexf(network, '../results/meme/test.gexf')

def gen_meme_path_v2(con, meme_id, output_path):
    query_sql = 'SELECT paper_id,  cited_paper_id, ' \
                '"HEALTH",UPPER(cited_paper_db), ' \
                'citing_paper_year, cited_paper_year ,' \
                'paper_ut, cited_paper_ut ' \
                'FROM health_meme_path WHERE meme_id = %s'
    cursor = con.cursor()
    cursor.execute(query_sql, (meme_id))
    results = cursor.fetchall()

    network = get_cooccurrance_network_v2(results, directed=True, source=0, target=1)

    year_dict = {}
    db_dict = {}
    ut_set = set()

    for row in results:
        paper_id = str(row[0])
        cited_paper_id = str(row[1])
        citing_paper_db = str(row[2])
        cited_paper_db = str(row[3])
        # citing_paper_year = row[4]
        # cited_paper_year = row[5]
        citing_paper_year = int(row[4])
        cited_paper_year = int(row[5])
        paper_ut = str(row[6]).strip()
        cited_paper_ut = str(row[7]).strip()

        ut_set.add(paper_ut)
        ut_set.add(cited_paper_ut)

        if paper_id not in year_dict:
            year_dict[paper_id] = citing_paper_year

        if paper_id not in db_dict:
            db_dict[paper_id] = citing_paper_db

        if cited_paper_id not in year_dict:
            year_dict[cited_paper_id] = cited_paper_year

        if cited_paper_id not in db_dict:
            db_dict[cited_paper_id] = cited_paper_db

    nx.set_node_attributes(network, year_dict, 'year')
    nx.set_node_attributes(network, db_dict, 'db')
    # nx.write_graphml(network, '{}/{}.graphml'.format(output_path, meme_id))
    # nx.write_pajek(network, '{}/{}.net'.format(output_path, meme_id))

    print(' OR UT='.join(list(ut_set)))
    # print()

    # properties_dict = gen_meme_path_properties_dict(network)
    properties_dict = {}

    return network, properties_dict

def run(con, output_path,limit=100):
    # query_sql = 'SELECT meme_id, ngram FROM all_tk_meme_with_year WHERE source = "health" ORDER BY meme_score DESC LIMIT %s'
    query_sql = 'SELECT meme_id, ngram FROM all_tk_meme_with_year WHERE source = "health" AND meme_id = 102699 ORDER BY meme_score DESC LIMIT %s'
    cursor = con.cursor()
    cursor.execute(query_sql, (limit))
    results = cursor.fetchall()

    # 单独做一个component size的
    component_size = []
    for row in results:
        print('正在生成{}:{}的路径'.format(row[0], row[1]))
        network, properties_dict = gen_meme_path_v2(con, row[0], output_path)
        component_size += list(map(len,nx.connected_components(network.to_undirected())))
    # with open(output_path + '/component_size.csv', mode='w', encoding='utf-8') as file:
    #     file.write('"component_size"\n')
    #     for i in component_size:
    #         file.write("{}\n".format(i))

    # 单独做一个度分布的
    # in_degree = []
    # out_degree = []
    # for row in results:
    #     print('正在生成{}:{}的路径'.format(row[0], row[1]))
    #     network, properties_dict = gen_meme_path_v2(con, row[0], output_path)
    #     raw_in = [i[1] for i in network.in_degree]
    #     raw_out = [i[1] for i in network.out_degree]
    #     in_degree += raw_in
    #     out_degree += raw_out
    #
    # with open(output_path + '/degree_distribution.csv', mode='w', encoding='utf-8') as file:
    #     file.write('"in_degree","out_degree"\n')
    #     for i,j in zip(in_degree, out_degree):
    #         file.write("{},{}\n".format(i,j))

    # 作图用的
    # with open(output_path + '/summary.csv', mode='w', encoding='utf-8') as file:
    #     file.write('"meme_id",'
    #                '"meme",'
    #                '"cascade_size",'
    #                '"cascade_depth",'
    #                '"max_comp_edge_count",'
    #                '"component_num",'
    #                '"source_num",'
    #                '"in_degree",'
    #                '"out_degree",'
    #                '"degree",'
    #                '"node_num",'
    #                '"edge_count",'
    #                '"clustering_coef",'
    #                '"density"\n')
        #原版的
        # file.write('"meme_id",'
        #            '"meme",'
        #            '"node_num",'
        #            '"edge_num",'
        #            '"max_in_degree",'
        #            '"min_in_degree",'
        #            '"avg_in_degree",'
        #            '"max_out_degree",'
        #            '"min_out_degree",'
        #            '"avg_out_degree",'
        #            '"density",'
        #            '"num_components",'
        #            '"max_component_diameter",'
        #            '"max_component_node_num",'
        #            '"max_component_edge_num",'
        #            '"max_component_max_in_degree",'
        #            '"max_component_min_in_degree",'
        #            '"max_component_avg_in_degree",'
        #            '"max_component_max_out_degree",'
        #            '"max_component_min_out_degree",'
        #            '"max_component_avg_out_degree"\n')



    # for row in results:
    #     print('正在生成{}:{}的路径'.format(row[0], row[1]))
    #     network, properties_dict = gen_meme_path_v2(con, row[0], output_path)
    #     with open(output_path + '/summary.csv', mode='a', encoding='utf-8', newline='') as file:
    #         writer = csv.writer(file)
    #         writer.writerow([row[0],
    #                          row[1],
    #                          properties_dict['cascade_size'],
    #                          properties_dict['cascade_depth'],
    #                          properties_dict['max_comp_edge_count'],
    #                          properties_dict['component_num'],
    #                          properties_dict['source_num'],
    #                          properties_dict['in_degree'],
    #                          properties_dict['out_degree'],
    #                          properties_dict['degree'],
    #                          properties_dict['node_num'],
    #                          properties_dict['edge_count'],
    #                          properties_dict['clustering_coef'],
    #                          properties_dict['density']])


            # writer.writerow([row[0],
            #                  row[1],
            #                  properties_dict['node_num'],
            #                  properties_dict['edge_num'],
            #                  properties_dict['max_in_degree'],
            #                  properties_dict['min_in_degree'],
            #                  properties_dict['avg_in_degree'],
            #                  properties_dict['max_out_degree'],
            #                  properties_dict['min_out_degree'],
            #                  properties_dict['avg_out_degree'],
            #                  properties_dict['density'],
            #                  properties_dict['num_components'],
            #                  properties_dict['max_component_diameter'],
            #                  properties_dict['max_component_node_num'],
            #                  properties_dict['max_component_edge_num'],
            #                  properties_dict['max_component_max_in_degree'],
            #                  properties_dict['max_component_min_in_degree'],
            #                  properties_dict['max_component_avg_in_degree'],
            #                  properties_dict['max_component_max_out_degree'],
            #                  properties_dict['max_component_min_in_degree'],
            #                  properties_dict['max_component_avg_in_degree']])




if __name__ == '__main__':
    con = get_db_connection('meme')
    meme = 'continual reassessment method'
    run(con, r'../results/meme/path', limit=999999)
    # gen_meme_path_v2(con, 102699, r'../results/meme/path/pajek')
    con.close()
