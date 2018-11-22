from DBUtil import get_db_connection
import time


def get_candidates(con):
    query_sql = 'SELECT `paper_n_gram`,`cited_paper_n_gram` FROM `cndblp_inner_reference_not_null`'
    cursor = con.cursor()
    cursor.execute(query_sql)

    result = cursor.fetchmany(1000)
    candidates_dict = {}
    i = len(result)
    while len(result) != 0:
        print('正在读取前{}条记录'.format(i))
        for row in result:
            paper_n_gram = row[0]
            cited_paper_n_gram = row[1]

            for p_gram in paper_n_gram.split(';'):
                if p_gram in candidates_dict:
                    candidates_dict[p_gram] += 1
                else:
                    candidates_dict[p_gram] = 1
            for c_gram in cited_paper_n_gram.split(';'):
                if c_gram in candidates_dict:
                    candidates_dict[c_gram] += 1
                else:
                    candidates_dict[c_gram] = 1

        print('原始候选词数量：', len(candidates_dict.keys()))
        result = cursor.fetchmany(1000)
        i += len(result)

    candidates_set = set()
    # print(sum(list(candidates_dict.values()))/len(candidates_dict))
    total_frequency = 0
    for key, value in candidates_dict.items():
        if value > 10:
            candidates_set.add((key, value))
            total_frequency += value
    del candidates_dict
    print('去掉词频小于等于10后的候选词数量：', len(candidates_set))
    return candidates_set, total_frequency


def cal_meme_score(con, candidates, delta=3):
    query_sql = 'SELECT `paper_id`,`paper_n_gram`,`cited_paper_n_gram` FROM `cndblp_inner_reference_n_gram`'
    cursor = con.cursor()
    cursor.execute(query_sql)
    results = cursor.fetchall()

    meme_score = {}

    i = 1
    for candidate in candidates:
        tick = time.time()
        if i % 100 == 0:
            print('正在计算第{}个meme的分数：{}'.format(i, candidate))
        i += 1
        dm_to_dm = 0
        d_to_dm = 0
        dm_to_d = 0
        d_to_d = 0

        # cursor.execute(query_sql)
        #
        # result = cursor.fetchmany(1000)
        # while len(result) != 0:
        #
        #     for row in result:

        for result in results:
            paper_id = result[0]
            paper_n_gram = result[1]
            cited_paper_n_gram = result[2]

            paper_n_gram = set(paper_n_gram.split(';'))
            cited_paper_n_gram = set(cited_paper_n_gram.split(';'))

            if candidate[0] in paper_n_gram:
                if candidate[0] in cited_paper_n_gram:
                    dm_to_dm += 1
                    d_to_dm += 1
                else:
                    dm_to_d += 1
                    d_to_d += 1
            else:
                if candidate[0] in cited_paper_n_gram:
                    d_to_dm += 1
                else:
                    d_to_d += 1

        # result = cursor.fetchmany(1000)
        cur_meme_score = (dm_to_dm / (d_to_dm + delta)) / ((dm_to_d + delta) / (d_to_d + delta))
        meme_score[candidate[0]] = (cur_meme_score, candidate[1])

        tok = time.time()
        if cur_meme_score != 0:
            print(candidate, dm_to_dm, d_to_dm, dm_to_d, d_to_d, cur_meme_score, tok - tick)

    return meme_score


if __name__ == '__main__':
    con = get_db_connection('patent_thesis')
    # candidates, total_frequency = get_candidates(con)
    # print(total_frequency)
    score = cal_meme_score(con, ['planar'], 3)
    con.close()
