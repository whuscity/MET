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


def gen_n_gram_v3(ngram=3):
    """
    生成N-Gram并存入数据库，先从内部引证表中读取有效的目标文献及其标题摘要，
    将标题摘要按以下操作步骤进行处理：小写化、（分句、分词、去停）、对每一句形成
    1到N-gram，建立N-gram序号字典，将处理好的结果存入数据库
    :param ngram: 最高需要多少-gram
    :return: None
    """
    stopwords = get_stopwords()

    query_title_abs_keyword_keywordplus = 'SELECT c.*, GROUP_CONCAT(d.KEYWORD) AS `keyword` FROM ' \
                                          '(SELECT a.`paper_id`, b.`title`, b.`abs` FROM ' \
                                          '(SELECT `paper_id` FROM `cndblp_paper` WHERE `paper_id` IN ' \
                                          '(SELECT DISTINCT `paper_id` FROM `cndblp_inner_reference`) OR `paper_id` IN ' \
                                          '(SELECT DISTINCT `cited_paper_id` FROM `cndblp_inner_reference`)) AS a ' \
                                          'LEFT JOIN `cndblp_paper` AS b ON a.`paper_id` = b.`paper_id`) AS c ' \
                                          'LEFT JOIN ' \
                                          '(SELECT PAPER_ID,KEYWORD FROM `cndblp_keyword_plus` ' \
                                          'UNION ALL ' \
                                          'SELECT PAPER_ID,KEYWORD FROM cndblp_keyword) AS d ON c.`paper_id` = d.`PAPER_ID` ' \
                                          'GROUP BY c.paper_id'
    # query_title_abs = 'SELECT a.paper_id, b.title, b.abs FROM ' \
    #                   '(SELECT `paper_id` FROM `cndblp_paper` ' \
    #                   'WHERE `paper_id` IN (SELECT DISTINCT `paper_id` FROM `cndblp_inner_reference`) ' \
    #                   'OR `paper_id` IN (SELECT DISTINCT `cited_paper_id` FROM `cndblp_inner_reference`)) AS a ' \
    #                   'LEFT JOIN `cndblp_paper` AS b ' \
    #                   'ON a.paper_id = b.paper_id'
    # insert_title = 'INSERT INTO `cndblp_title_ngram` VALUES(%s, %s)'
    insert_abs = 'INSERT INTO `cndblp_abs_ngram` VALUES(%s, %s)'
    # insert_keyword = 'INSERT INTO `cndblp_keyword_ngram` VALUES(%s, %s)'
    insert_word = 'INSERT INTO `cndblp_ngram_list` VALUES(%s, %s)'
    insert_title_keyword = 'INSERT INTO `cndblp_title_keyword_ngram` VALUES(%s, %s)'

    title_list = []
    abs_list = []
    keyword_list = []
    word_dict = {}
    key = 1

    # 开启两个连接，一个用于分批查询，另一个用于在查询过程中插入数据
    con = get_db_connection('patent_thesis')
    con2 = get_db_connection('patent_thesis')

    cursor = con.cursor()
    cursor2 = con.cursor()

    # 每次取1000条进行处理
    batch_size = 1000
    cursor.execute(query_title_abs_keyword_keywordplus)
    result = cursor.fetchmany(batch_size)

    i = len(result)
    while len(result) > 0:
        print('正在处理第{}条以前的文献'.format(i))

        # 对每批1000条的数据进行逐条处理
        for row in result:
            paper_id = row[0]
            title = row[1].lower()
            abs = row[2]
            keyword = row[3]

            # 如果有的文献没有摘要或关键词，则为空
            if abs is not None:
                abs = abs.lower()
            else:
                abs = ''

            if keyword is not None:
                keyword = keyword.lower()
            else:
                keyword = ''

            # 对标题进行分词、去停处理（标题不需要分句）
            title = title.replace('"', '')
            title = [word for word in word_tokenize(title) if word not in stopwords and len(word) > 1]
            title_ngram = []

            for n in range(1, ngram + 1):
                tmp = [' '.join(grams) for grams in ngrams(title, n)]
                title_ngram += tmp

            # 对摘要进行分句、分词、去停处理，摘要的ngram要在每个句子内部进行
            abs_ngram = []

            abs = abs.replace('"', '')
            for sentence in split_sentence(abs):
                s = [word for word in word_tokenize(sentence) if word not in stopwords and len(word) > 1]
                for n in range(1, ngram + 1):
                    tmp2 = [' '.join(grams) for grams in ngrams(s, n)]
                    abs_ngram += tmp2

            # 对关键词进行拆分处理，不需要ngram
            keyword = keyword.split(',')
            for _keyword in keyword:
                if _keyword not in word_dict:
                    word_dict[_keyword] = key
                    key += 1
                _keyword_index = word_dict[_keyword]
                keyword_list.append((paper_id, _keyword_index))

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


        cursor2.executemany(insert_title_keyword, keyword_list)
        cursor2.executemany(insert_title_keyword, title_list)
        cursor2.executemany(insert_abs, abs_list)


        # 记得清空待插入列表
        title_list.clear()
        abs_list.clear()
        keyword_list.clear()

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
