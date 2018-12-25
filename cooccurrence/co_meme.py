from DBUtil import get_db_connection
from netUtil.cooccurrence_network import *
import networkx as nx
import os

def gen_meme_cooccurrence_network(con):
    query_sql = 'SELECT `source1`, `source2` FROM `meme_cooccurrence`'

    cursor = con.cursor()
    cursor.execute(query_sql)
    result = cursor.fetchall()

    sources = generate_matrix_index(result)
    co_network = get_cooccurrance_network(sources, result, 'SOURCE')

    filename = '../results/meme/cooccurrence/co_network.gexf'
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    nx.write_gexf(co_network, filename)

    return co_network

if __name__ == '__main__':
    con = get_db_connection('meme')
    gen_meme_cooccurrence_network(con)
    con.close()