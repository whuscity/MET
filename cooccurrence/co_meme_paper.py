from DBUtil import get_db_connection
from netUtil.cooccurrence_network import *
from netUtil.network_output import *
from netUtil.add_network_properties import *


def gen_meme_paper_cooccurrence_network(con):
    query_sql = 'SELECT `source1`, `source2` FROM `top500_meme_paper_cooccurrence`'

    cursor = con.cursor()
    cursor.execute(query_sql)
    result = cursor.fetchall()

    network = get_cooccurrance_network_v2(result)
    assert network is not None

    network = add_degree_centrality(network)

    nodes_filename = '../results/meme/cooccurrence/meme_paper/top500-nodes.csv'
    edges_filename = '../results/meme/cooccurrence/meme_paper/top500-edges.csv'
    info_filename = '../results/meme/cooccurrence/meme_paper/top500-info.csv'
    gexf_filename = '../results/meme/cooccurrence/meme_paper/top500.gexf'

    csv_output(network,nodes_filename,edges_filename,info_filename)
    gexf_output(network, gexf_filename)
    return network

if __name__ == '__main__':
    con = get_db_connection('meme')
    gen_meme_paper_cooccurrence_network(con)
    con.close()