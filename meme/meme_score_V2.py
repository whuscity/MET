from DBUtil import get_db_connection
import time


def get_candidates(con):
    #TODO:需要注意的是，标题和摘要的联合计算还没有实现，目前仅能分别计算
    query_ngram = 'SELECT b.nid, b.ngram FROM cndblp_abs_ngram AS a ' \
                  'LEFT JOIN cndblp_ngram_list AS b ' \
                  'ON a.ngram = b.nid GROUP BY b.nid LIMIT 10000'

    cursor = con.cursor()
    cursor.execute(query_ngram)
    results = cursor.fetchall()

    return results


def cal_meme_score(con, candidates, delta=3):
    """
    根据论文计算Meme score
    :param con: 数据库连接
    :param candidates: 待评分meme列表
    :param delta: 参数
    :return: None
    """
    query_d_to_dm = 'SELECT COUNT(DISTINCT a.PAPER_ID) FROM cndblp_inner_reference AS a ' \
                    'LEFT JOIN cndblp_abs_ngram AS b ' \
                    'ON a.cited_paper_id = b.paper_id WHERE ngram = {}'

    query_dm = 'SELECT COUNT(DISTINCT a.PAPER_ID) FROM cndblp_inner_reference AS a ' \
               'LEFT JOIN cndblp_abs_ngram AS b ' \
               'ON a.paper_id = b.paper_id WHERE ngram={}'

    query_dm_to_dm = 'SELECT COUNT(DISTINCT citing_paper_id) FROM dm_to_dm ' \
                     'WHERE citing_ngram = {} AND cited_ngram = {}'

    # query_total_paper_num = 'SELECT COUNT(*) FROM `cndblp_paper`'
    # 有BUG，文章总数应该是来自内部引证表的施引、参考文献两列的DISTINCT（已修复）
    query_total_paper_num = 'SELECT COUNT(*) FROM ' \
                            '(SELECT `paper_id` FROM `cndblp_paper` ' \
                            'WHERE `paper_id` IN (SELECT DISTINCT `paper_id` FROM `cndblp_inner_reference`) ' \
                            'OR `paper_id` IN (SELECT DISTINCT `cited_paper_id` FROM `cndblp_inner_reference`)) AS a ' \
                            'LEFT JOIN `cndblp_paper` AS b ' \
                            'ON a.paper_id = b.paper_id'

    query_ngram_occur_paper = 'SELECT COUNT(ngram) FROM cndblp_abs_ngram WHERE ngram = {} GROUP BY ngram'

    cursor = con.cursor()
    cursor.execute(query_total_paper_num)

    total_paper_num = cursor.fetchall()[0][0]
    print('文章总数：', total_paper_num)

    i = 1

    score_result = []

    tik = time.time()
    for candidate in candidates:

        _query_d_to_dm = query_d_to_dm.format(candidate[0])
        _query_dm = query_dm.format(candidate[0])
        _query_dm_to_dm = query_dm_to_dm.format(candidate[0], candidate[0])
        _query_ngram_occur_paper = query_ngram_occur_paper.format(candidate[0])

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
            with open('result.csv', 'a', encoding='utf-8') as file:
                for value in score_result:
                    file.write(','.join(list(map(str, value))))
                    file.write('\n')
            score_result.clear()

        i += 1
    with open('result.csv', 'a', encoding='utf-8') as file:
        for value in score_result:
            file.write(','.join(list(map(str, value))))
            file.write('\n')


if __name__ == '__main__':
    con = get_db_connection('patent_thesis')
    print('正在获取候选词列表')
    candidates = get_candidates(con)
    print('候选词列表获取完成，开始计算meme分数')
    with open('result.csv', 'w', encoding='utf-8') as file:
        file.write('nid,ngram,dm_to_dm, d_to_dm, dm_to_d, d_to_d, ngram_occur_num,total_paper_num, meme_score\n')

    cal_meme_score(con, candidates)

    # cal_meme_score(con, [('4663262','disilicide films')])
    con.close()
