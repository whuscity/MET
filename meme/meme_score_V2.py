from DBUtil import get_db_connection
import time
import os


def get_candidates(con):
    # TODO:需要注意的是，标题和摘要的联合计算还没有实现，目前仅能分别计算
    query_abs_ngram = 'SELECT b.nid, b.ngram FROM cndblp_abs_ngram AS a ' \
                      'LEFT JOIN cndblp_ngram_list AS b ' \
                      'ON a.ngram = b.nid GROUP BY b.nid'
    # query_title_ngram = 'SELECT b.nid, b.ngram FROM cndblp_title_ngram AS a ' \
    #                     'LEFT JOIN cndblp_ngram_list AS b ' \
    #                     'ON a.ngram = b.nid GROUP BY b.nid'
    # query_keyword_ngram = 'SELECT b.nid, b.ngram FROM cndblp_keyword_ngram AS a ' \
    #                     'LEFT JOIN cndblp_ngram_list AS b ' \
    #                     'ON a.ngram = b.nid GROUP BY b.nid'

    query_title_keyword_ngram = 'SELECT b.nid, b.ngram FROM cndblp_title_keyword_ngram AS a ' \
                        'LEFT JOIN cndblp_ngram_list AS b ' \
                        'ON a.ngram = b.nid GROUP BY b.nid'

    # cursor = con.cursor()
    # cursor.execute(query_title_ngram)
    # title_results = cursor.fetchall()
    # cursor.execute(query_keyword_ngram)
    # keyword_results = cursor.fetchall()
    #
    # result_set = set()
    # for title_ngram in title_results:
    #     result_set.add(title_ngram)
    # for keyword in keyword_results:
    #     result_set.add(keyword)
    #
    # result_set = list(result_set)
    # result_set.sort(key=lambda x:x[0])
    result = set()
    cursor = con.cursor()
    cursor.execute(query_title_keyword_ngram)
    tk_result = cursor.fetchall()
    cursor.execute(query_abs_ngram)
    a_result = cursor.fetchall()

    for tk_ngram in tk_result:
        result.add(tk_ngram)
    for a_ngram in a_result:
        result.add(a_ngram)
    result = list(result)
    result.sort(key= lambda x:x[0])
    return result


def cal_meme_score(con, candidates, output_path, delta=3):
    """
    根据论文计算Meme score
    :param con: 数据库连接
    :param candidates: 待评分meme列表
    :param delta: 参数
    :return: None
    """
    # query_d_to_dm = 'SELECT COUNT(DISTINCT a.PAPER_ID) FROM cndblp_inner_reference AS a ' \
    #                 'LEFT JOIN cndblp_title_keyword_ngram AS b ' \
    #                 'ON a.cited_paper_id = b.paper_id WHERE ngram = {}'

    query_d_to_dm = 'SELECT COUNT(DISTINCT PAPER_ID) FROM ' \
                    '(SELECT a.PAPER_ID FROM cndblp_inner_reference AS a ' \
                    'LEFT JOIN ' \
                    'cndblp_title_keyword_ngram AS b ON a.cited_paper_id = b.paper_id WHERE ngram = {} ' \
                    'UNION ALL ' \
                    'SELECT c.PAPER_ID FROM cndblp_inner_reference AS c ' \
                    'LEFT JOIN ' \
                    'cndblp_abs_ngram AS d ON c.cited_paper_id = d.paper_id WHERE ngram = {}) AS e'

    # query_dm = 'SELECT COUNT(DISTINCT a.PAPER_ID) FROM cndblp_inner_reference AS a ' \
    #            'LEFT JOIN cndblp_title_keyword_ngram AS b ' \
    #            'ON a.paper_id = b.paper_id WHERE ngram={}'

    query_dm = 'SELECT COUNT(DISTINCT PAPER_ID) FROM ' \
               '(SELECT a.PAPER_ID FROM cndblp_inner_reference AS a ' \
               'LEFT JOIN ' \
               'cndblp_title_keyword_ngram AS b ON a.paper_id = b.paper_id WHERE ngram = {} ' \
               'UNION ALL ' \
               'SELECT c.PAPER_ID FROM cndblp_inner_reference AS c ' \
               'LEFT JOIN cndblp_abs_ngram AS d ON c.paper_id = d.paper_id WHERE ngram = {}) AS e'

    # query_dm_to_dm = 'SELECT COUNT(DISTINCT a.PAPER_ID) FROM cndblp_inner_reference AS a ' \
    #                  'INNER JOIN cndblp_title_keyword_ngram AS b ON a.paper_id = b.paper_id ' \
    #                  'INNER JOIN cndblp_title_keyword_ngram AS c ON a.cited_paper_id = c.paper_id ' \
    #                  'WHERE b.ngram = {} AND c.ngram = {}'

    query_dm_to_dm = 'SELECT COUNT(DISTINCT paper_id) FROM ' \
                     '(SELECT a.PAPER_ID FROM cndblp_inner_reference AS a ' \
                     'INNER JOIN cndblp_title_keyword_ngram AS b ON a.paper_id = b.paper_id ' \
                     'INNER JOIN cndblp_title_keyword_ngram AS c ON a.cited_paper_id = c.paper_id WHERE b.ngram = {} AND c.ngram = {} ' \
                     'UNION ALL ' \
                     'SELECT d.PAPER_ID FROM cndblp_inner_reference AS d ' \
                     'INNER JOIN cndblp_abs_ngram AS e ON d.paper_id = e.paper_id ' \
                     'INNER JOIN cndblp_abs_ngram AS f ON d.cited_paper_id = f.paper_id WHERE e.ngram = {} AND f.ngram = {}) AS g'

    # query_total_paper_num = 'SELECT COUNT(*) FROM `cndblp_paper`'
    # 有BUG，文章总数应该是来自内部引证表的施引、参考文献两列的DISTINCT（已修复）
    query_total_paper_num = 'SELECT COUNT(*) FROM ' \
                            '(SELECT `paper_id` FROM `cndblp_paper` ' \
                            'WHERE `paper_id` IN (SELECT DISTINCT `paper_id` FROM `cndblp_inner_reference`) ' \
                            'OR `paper_id` IN (SELECT DISTINCT `cited_paper_id` FROM `cndblp_inner_reference`)) AS a ' \
                            'LEFT JOIN `cndblp_paper` AS b ' \
                            'ON a.paper_id = b.paper_id'

    query_ngram_occur_paper = 'SELECT COUNT(DISTINCT paper_id) FROM ' \
                              '(SELECT * FROM cndblp_title_keyword_ngram WHERE ngram = {} ' \
                              'UNION ALL ' \
                              'SELECT * FROM cndblp_abs_ngram WHERE ngram = {}) AS a'

    # query_ngram_occur_paper = 'SELECT COUNT(DISTINCT paper_id) FROM ' \
    #                           '`cndblp_title_keyword_ngram` WHERE ngram = {}'

    cursor = con.cursor()
    cursor.execute(query_total_paper_num)

    total_paper_num = cursor.fetchall()[0][0]
    print('文章总数：', total_paper_num)

    i = 1

    score_result = []

    tik = time.time()
    for candidate in candidates:

        _query_d_to_dm = query_d_to_dm.format(candidate[0],candidate[0])
        _query_dm = query_dm.format(candidate[0],candidate[0])
        _query_dm_to_dm = query_dm_to_dm.format(candidate[0], candidate[0],candidate[0],candidate[0])
        _query_ngram_occur_paper = query_ngram_occur_paper.format(candidate[0],candidate[0])

        cursor.execute(_query_d_to_dm)
        d_to_dm = cursor.fetchall()[0][0]
        assert d_to_dm >= 0

        # print(d_to_dm)

        cursor.execute(_query_dm)
        dm = cursor.fetchall()[0][0]

        # print(dm)

        cursor.execute(_query_dm_to_dm)
        dm_to_dm = cursor.fetchall()[0][0]

        # print(dm_to_dm)

        cursor.execute(_query_ngram_occur_paper)
        ngram_occur_num = cursor.fetchall()[0][0]
        freq = ngram_occur_num / total_paper_num

        dm_to_d = dm - dm_to_dm
        assert dm_to_d >= 0
        d_to_d = total_paper_num - d_to_dm
        assert d_to_d >= 0

        m_score = (dm_to_dm / (d_to_dm + delta)) / ((dm_to_d + delta) / (d_to_d + delta))
        meme_score = m_score * freq

        span = 100

        if i % span == 0:
            tok = time.time()
            print('第{}个候选词计算完成：{}'.format(i, candidate))
            print('dm_to_dm:{}, d_to_dm:{}, dm_to_d:{}, d_to_d:{}, occur_num:{}, total_num:{}'.format(dm_to_dm, d_to_dm,
                                                                                                      dm_to_d, d_to_d,
                                                                                                      ngram_occur_num,
                                                                                                      total_paper_num))
            print('meme score:{}, avg. time:{}'.format(meme_score, (tok - tik) / span))
            print('=======================')
            print()
            tik = time.time()

        score_result.append((candidate[0], candidate[1], dm_to_dm, d_to_dm, dm_to_d, d_to_d, ngram_occur_num,
                             total_paper_num, meme_score))
        if len(score_result) > 1000:
            with open(output_path, 'a', encoding='utf-8') as file:
                for value in score_result:
                    file.write('"')
                    file.write('","'.join(list(map(str, value))))
                    file.write('"\n')
            score_result.clear()

        i += 1
    with open(output_path, 'a', encoding='utf-8') as file:
        for value in score_result:
            file.write('"')
            file.write('","'.join(list(map(str, value))))
            file.write('"\n')


if __name__ == '__main__':
    conf = 'health_statistics'
    con = get_db_connection(conf)
    print('正在获取候选词列表')
    candidates = get_candidates(con)
    print(candidates[:5])
    print('候选词列表获取完成，共有{}个候选词，开始计算meme分数'.format(str(len(candidates))))

    filename = '../results/meme/{}.csv'.format(conf)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(
            '"nid","ngram","dm_to_dm","d_to_dm","dm_to_d","d_to_d","ngram_occur_num","total_paper_num","meme_score"\n')

    cal_meme_score(con, candidates, filename, delta=3)

    con.close()
