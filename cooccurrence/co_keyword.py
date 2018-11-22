from DBUtil import *
from nltk.stem.wordnet import WordNetLemmatizer
from itertools import combinations
import networkx as nx


def split_and_process_keywords(con):
    """
    对已经导入到数据库中的文献进行关键词拆分和词形还原处理，
    然后将关键词以分号分隔整合后存入数据库

    :param con: 数据库连接对象
    :return: None
    """
    cursor = con.cursor()
    cursor.execute('SELECT `sid`, `keywords`, `keywords_plus`, `year` FROM t_nedd')

    print('共有{}篇文献的关键词需要拆分'.format(cursor.rowcount))

    result = cursor.fetchall()
    insert_sql = 'INSERT INTO t_nedd_keywords(`nedd_sid`, `keyword`, `year`) VALUES(%s, %s, %s)'

    lemmatizer = WordNetLemmatizer()
    processed_keywords = []

    i = 0
    for row in result:
        sid = row[0]
        keywords = row[1]
        keyword_plus = row[2]
        year = row[3]

        """
        关键词字段不为空的话，就按分号隔开取出其中的所有关键词（或关键词短语）
        每一个关键词（或关键词短语），对于里面具体的单词进行词形还原
        因为词形还原只能对单词进行，所以短语需要先拆开再合并
        处理好的关键词（或关键词短语）存放在processed_keywords列表中
        """
        if keywords is not None:
            keywords = keywords.split(';')

            for keyword in keywords:
                processed_keyword = (
                    ' '.join(list(map(lemmatizer.lemmatize, keyword.lower().strip().split(' '))))).lower()
                processed_keywords.append((sid, processed_keyword.strip(), year))

        if keyword_plus is not None:
            keyword_plus = keyword_plus.split(';')

            for keyword in keyword_plus:
                processed_keyword = (
                    ' '.join(list(map(lemmatizer.lemmatize, keyword.lower().strip().split(' '))))).lower()
                processed_keywords.append((sid, processed_keyword.strip(), year))

        if keywords is None and keyword_plus is None:
            continue

        i += 1
        if i % 100 == 0:
            print('正在处理第{}篇文章的关键词：{} -> {}'.format(sid, keyword, processed_keyword))

    print('\t处理完成，正在保存到数据库')
    try:
        cursor.executemany(insert_sql, processed_keywords)
        con.commit()
        print('\t保存完成')
    except Exception as e:
        con.rollback()
        print('\t保存出错，回滚')
        print(e)


def keyword_cooccurrence(con):
    """
    基于一对多的关键词表，首先得到所有的文章ID，
    然后根据文章ID来查到该文章所有的关键词，
    使用itertools.combinations来获得这些关键词的两两组合，
    连同文章ID和年份一起存入关键词共现表，以方便时序分析

    :param con: 数据库连接对象
    :return: None
    """
    cursor = con.cursor()

    keywords_sql = 'SELECT `nedd_sid`, GROUP_CONCAT(`keyword` SEPARATOR \';\'), `year` FROM t_nedd_keywords GROUP BY `nedd_sid`'
    insert_sql = 'INSERT INTO t_nedd_keyword_cooccurrence (`nedd_sid`, `keyword1`, `keyword2`,  `year`) VALUES (%s, %s, %s, %s)'
    cursor.execute(keywords_sql)
    result = cursor.fetchall()

    insert_list = []
    i = 1
    for paper in result:
        nedd_sid = paper[0]
        keywords = paper[1].split(';')
        year = paper[2]

        combine = list(combinations(keywords, 2))

        for keyword_pair in combine:
            insert_list.append((nedd_sid, keyword_pair[0], keyword_pair[1], year))

        if i % 100 == 0:
            print('正在生成第{}篇文章关键词的共现组合'.format(nedd_sid))
        i += 1

        if len(insert_list) > 50000:
            print('\t正在将缓冲区的{}条数据保存到数据库'.format(len(insert_list)))
            try:
                cursor.executemany(insert_sql, insert_list)
                con.commit()
                insert_list.clear()
                print('\t保存完成，清空缓冲区')
            except Exception as e:
                con.rollback()
                print('\t保存出错，回滚')
                print(e)

    if len(insert_list) > 0:
        try:
            print('正在将缓冲区中剩余的{}条共现组合保存到数据库'.format(len(insert_list)))
            print('共处理了{}篇文献'.format(i))
            cursor.executemany(insert_sql, insert_list)
            con.commit()
        except Exception as e:
            con.rollback()
            print('保存出错，正在回滚')
            print(e)


def generate_graph(con):
    """
    利用关键词表获得全部关键词及其出现次数，作为图的节点及其权重
    利用关键词共现表获得全部共现组合，将其重复的部分聚合，权重相加，作为图的边及其权重
    利用networkx加载节点和边，并保存成gexf格式可以供gephi直接使用

    :param con:
    :return:
    """
    cursor = con.cursor()
    G = nx.Graph()
    year_range = list(range(2000, 2013, 1))

    for year in year_range:

        print('正在生成第{}年的共现网络'.format(year))

        query_nodes = 'SELECT `keyword`, COUNT(*) FROM t_nedd_keywords WHERE `year` = {} GROUP BY `keyword` ORDER BY `keyword`'.format(
            year)
        query_edges = 'SELECT `keyword1`, `keyword2`, COUNT(*) FROM t_nedd_keyword_cooccurrence WHERE `year` = {} GROUP BY `keyword1`, `keyword2`'.format(
            year)

        """
        nodes为节点按顺序的编号
        label_dict存放节点编号与其所代表的关键词文本的对应
        num_dict存放节点编号与其所代表的关键词出现数量的对应
        reverse_label_dict存放节点关键词文本与其节点编号的对应
        """
        cursor.execute(query_nodes)
        node_num = cursor.rowcount
        nodes = list(range(1, node_num + 1))

        labels = cursor.fetchall()
        label_dict = {}
        num_dict = {}
        reverse_label_dict = {}

        for i in range(len(labels)):
            label_dict[i + 1] = labels[i][0]
            num_dict[i + 1] = labels[i][1]
            reverse_label_dict[labels[i][0]] = i + 1

        """
        批量添加节点并为每一个节点赋予文本和数量属性
        """
        G.add_nodes_from(nodes)
        nx.set_node_attributes(G, label_dict, 'keyword')
        nx.set_node_attributes(G, num_dict, 'num')

        cursor.execute(query_edges)
        keyword_pairs = cursor.fetchall()

        """
        因为是无向图，所以双向的节点联系应该转变为权重叠加（其实gephi有这个功能）
        先判断边是否存在，不存在则新建，存在则叠加权重（has_edge函数在无向图中无视节点顺序，所以不需要写or）
        """
        for keyword_pair in keyword_pairs:
            if G.has_edge(reverse_label_dict[keyword_pair[0]], reverse_label_dict[keyword_pair[1]]):
                G.edges[reverse_label_dict[keyword_pair[0]], reverse_label_dict[keyword_pair[1]]]['Weight'] += \
                    keyword_pair[2]
            else:
                G.add_edge(reverse_label_dict[keyword_pair[0]], reverse_label_dict[keyword_pair[1]],
                           Weight=keyword_pair[2])
        nx.write_gexf(G, str(year) + '关键词共现网络.gexf')
        print('生成完成')


if __name__ == '__main__':
    connection = get_db_connection('MET')
    # split_and_process_keywords(connection)
    # keyword_cooccurrence(connection)
    # generate_graph(connection)
    connection.close()
