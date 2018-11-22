from DBUtil import get_db_connection
from nltk import ngrams
from nltk.tokenize import word_tokenize
import nltk.data


def split_sentence(text):
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    sentences = tokenizer.tokenize(text)
    return sentences


def get_stopwords(path='../conf/stopwords.txt'):
    stopwords = []
    with open(path, encoding='utf-8') as file:
        for row in file:
            stopwords.append(row.strip())
    return stopwords


def gen_n_gram_v1():
    # 读取停用词表
    stopwords = get_stopwords()

    # 从数据库中读取目标文献标题+摘要以及参考文献的标题，去除停用词和部分标点后生成N-gram，存回数据库
    con = get_db_connection('patent_thesis')
    con2 = get_db_connection('patent_thesis')
    cursor = con.cursor()
    query_sql = 'SELECT paper_id, cited_paper_id, paper_text, cited_paper_title FROM `cndblp_inner_reference_not_null`'
    update_sql = 'UPDATE `cndblp_inner_reference_not_null` SET paper_n_gram = %s, cited_paper_n_gram = %s WHERE paper_id = %s AND cited_paper_id = %s'
    cursor.execute(query_sql)

    result = cursor.fetchmany(1000)

    cursor2 = con2.cursor()
    i = len(result)
    while len(result) != 0:
        print('正在处理第{}条以前的文献'.format(i))
        update_list = []
        for row in result:
            paper_id = row[0]
            cited_paper_id = row[1]
            paper_text = row[2].replace('.', '').replace(',', '').replace('(', '').replace(')', '').replace(':',
                                                                                                            '').lower().split()
            cited_paper_text = row[3].replace('.', '').replace(',', '').replace('(', '').replace(')', '').replace(':',
                                                                                                                  '').lower().split()

            paper_text = [word for word in paper_text if word not in stopwords]
            cited_paper_text = [word for word in cited_paper_text if word not in stopwords]

            paper_n_gram = []
            cited_paper_n_gram = []

            for n in range(1, 4):
                tmp = [' '.join(grams) for grams in ngrams(paper_text, n)]
                tmp2 = [' '.join(grams) for grams in ngrams(cited_paper_text, n)]

                paper_n_gram += tmp
                cited_paper_n_gram += tmp2
            paper_n_gram = set(paper_n_gram)
            cited_paper_n_gram = set(cited_paper_n_gram)

            update_list.append((';'.join(paper_n_gram), ';'.join(cited_paper_n_gram), paper_id, cited_paper_id))
        cursor2.executemany(update_sql, update_list)

        result = cursor.fetchmany(1000)
        i += len(result)

    con.close()
    con2.close()


def gen_n_gram_v2():
    # 读取停用词表
    stopwords = get_stopwords()

    # 有BUG，不应该从全部文献中取标题和摘要，应该只选择那些出现在内部引证关系表中的文献（已经修复）

    query_sql = 'SELECT a.paper_id, b.title, b.abs FROM ' \
                '(SELECT `paper_id` FROM `cndblp_paper` ' \
                'WHERE `paper_id` IN (SELECT DISTINCT `paper_id` FROM `cndblp_inner_reference`) ' \
                'OR `paper_id` IN (SELECT DISTINCT `cited_paper_id` FROM `cndblp_inner_reference`)) AS a ' \
                'LEFT JOIN `cndblp_paper` AS b ' \
                'ON a.paper_id = b.paper_id'
    insert_title_sql = 'INSERT INTO `cndblp_title_ngram` VALUES(%s, %s)'
    insert_abs_sql = 'INSERT INTO `cndblp_abs_ngram` VALUES(%s, %s)'
    insert_ngram_list_sql = 'INSERT INTO `cndblp_ngram_list` VALUES(%s, %s)'
    title_insert_list = []
    abs_insert_list = []
    ngram_dict = {}
    key = 1

    con = get_db_connection('patent_thesis')
    con2 = get_db_connection('patent_thesis')
    cursor = con.cursor()
    cursor2 = con2.cursor()
    cursor.execute(query_sql)

    result = cursor.fetchmany(1000)

    i = len(result)
    while len(result) > 0:
        print('正在处理第{}条以前的文献'.format(i))
        for row in result:
            paper_id = row[0]
            title = row[1].replace('.', '').replace(',', '').replace('(', '').replace(')', '').replace(':',
                                                                                                       '').lower().split()
            abs = row[2]
            if abs is not None:
                abs = abs.replace('.', '').replace(',', '').replace('(', '').replace(')', '').replace(':',
                                                                                                      '').lower().split()
            else:
                abs = ''

            title = [word for word in title if word not in stopwords]
            abs = [word for word in abs if word not in stopwords]

            title_ngram = []
            abs_ngram = []

            for n in range(1, 4):
                tmp = [' '.join(grams) for grams in ngrams(title, n)]
                tmp2 = [' '.join(grams) for grams in ngrams(abs, n)]

                title_ngram += tmp
                abs_ngram += tmp2

            title_ngram = set(title_ngram)
            abs_ngram = set(abs_ngram)

            for t_ngram in title_ngram:
                if t_ngram not in ngram_dict:
                    ngram_dict[t_ngram] = key
                    key += 1
                t_index = ngram_dict[t_ngram]
                title_insert_list.append((paper_id, t_index))
            for a_ngram in abs_ngram:
                if a_ngram not in ngram_dict:
                    ngram_dict[a_ngram] = key
                    key += 1
                a_index = ngram_dict[a_ngram]
                abs_insert_list.append((paper_id, a_index))

        cursor2.executemany(insert_title_sql, title_insert_list)
        cursor2.executemany(insert_abs_sql, abs_insert_list)

        # 记得每次插入完成要清空待插入列表
        title_insert_list.clear()
        abs_insert_list.clear()

        result = cursor.fetchmany(1000)
        i += len(result)

    insert_ngram_list = []
    for key, value in ngram_dict.items():
        insert_ngram_list.append((value, key))

    del ngram_dict

    cursor2.executemany(insert_ngram_list_sql, insert_ngram_list)

    con.close()
    con2.close()


def gen_n_gram_v3(ngram=3):
    """
    生成N-Gram并存入数据库，先从内部引证表中读取有效的目标文献及其标题摘要，
    将标题摘要按以下操作步骤进行处理：小写化、（分句、分词、去停）、对每一句形成
    1到N-gram，建立N-gram序号字典，将处理好的结果存入数据库
    :param ngram: 最高需要多少-gram
    :return: None
    """
    stopwords = get_stopwords()

    query_title_abs = 'SELECT a.paper_id, b.title, b.abs FROM ' \
                      '(SELECT `paper_id` FROM `cndblp_paper` ' \
                      'WHERE `paper_id` IN (SELECT DISTINCT `paper_id` FROM `cndblp_inner_reference`) ' \
                      'OR `paper_id` IN (SELECT DISTINCT `cited_paper_id` FROM `cndblp_inner_reference`)) AS a ' \
                      'LEFT JOIN `cndblp_paper` AS b ' \
                      'ON a.paper_id = b.paper_id'
    insert_title = 'INSERT INTO `cndblp_title_ngram` VALUES(%s, %s)'
    insert_abs = 'INSERT INTO `cndblp_abs_ngram` VALUES(%s, %s)'
    insert_word = 'INSERT INTO `cndblp_ngram_list` VALUES(%s, %s)'

    title_list = []
    abs_list = []
    word_dict = {}
    key = 1

    # 开启两个连接，一个用于分批查询，另一个用于在查询过程中插入数据
    con = get_db_connection('patent_thesis')
    con2 = get_db_connection('patent_thesis')

    cursor = con.cursor()
    cursor2 = con.cursor()

    # 每次取1000条进行处理
    batch_size = 1000
    cursor.execute(query_title_abs)
    result = cursor.fetchmany(batch_size)

    i = len(result)
    while len(result) > 0:
        print('正在处理第{}条以前的文献'.format(i))

        # 对每批1000条的数据进行逐条处理
        for row in result:
            paper_id = row[0]
            title = row[1].lower()
            abs = row[2]

            # 如果有的文献没有摘要，则摘要为空
            if abs is not None:
                abs = abs.lower()
            else:
                abs = ''

            # 对标题进行分词、去停处理（标题不需要分句）
            title = [word for word in word_tokenize(title) if word not in stopwords and len(word) > 1]
            title_ngram = []

            for n in range(1, ngram + 1):
                tmp = [' '.join(grams) for grams in ngrams(title, n)]
                title_ngram += tmp

            # 对摘要进行分句、分词、去停处理，摘要的ngram要在每个句子内部进行
            abs_ngram = []

            for sentence in split_sentence(abs):
                s = [word for word in word_tokenize(sentence) if word not in stopwords and len(word) > 1]
                for n in range(1, ngram + 1):
                    tmp2 = [' '.join(grams) for grams in ngrams(s, n)]
                    abs_ngram += tmp2

            # 去除重复的ngram
            title_ngram = set(title_ngram)
            abs_ngram = set(abs_ngram)

            for t_ngram in title_ngram:
                if t_ngram not in word_dict:
                    word_dict[t_ngram] = key
                    key += 1
                t_index = word_dict[t_ngram]
                title_list.append((paper_id, t_index))
            for a_ngram in abs_ngram:
                if a_ngram not in word_dict:
                    word_dict[a_ngram] = key
                    key += 1
                a_index = word_dict[a_ngram]
                abs_list.append((paper_id, a_index))

        cursor2.executemany(insert_title, title_list)
        cursor2.executemany(insert_abs, abs_list)

        # 记得清空待插入列表
        title_list.clear()
        abs_list.clear()

        result = cursor.fetchmany(batch_size)
        i += len(result)

    print('处理完成，正在插入ngram索引')
    word_list = []
    for key, value in word_dict.items():
        word_list.append((value, key))
    del word_dict

    cursor2.executemany(insert_word, word_list)

    con.close()
    con2.close()
    print('全部完成')


if __name__ == '__main__':
    gen_n_gram_v3(3)
