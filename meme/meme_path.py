from DBUtil import get_db_connection
from netUtil.cooccurrence_network import generate_matrix_index, get_cooccurrance_network
import networkx as nx


def gen_meme_path(con):
    query_sql = 'SELECT UPPER(CONCAT_WS("_", paper_id, cited_paper_db)), UPPER(CONCAT_WS("_", cited_paper_id, cited_paper_db)), ' \
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


if __name__ == '__main__':
    con = get_db_connection('meme')
    gen_meme_path(con)
    con.close()
