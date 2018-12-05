import pandas as pd
from math import isclose
import os

filenames = ['health', 'health_cs', 'health_care', 'health_statistics', 'health_medicine']
n = 20

for filename in filenames:
    file_path = r'../results/meme/{}_finished.csv'.format(filename)
    df = pd.read_csv(file_path)
    print('{}原始meme个数：{}'.format(filename, len(df)))

    df = df.dropna()
    df = df.drop(df.index[df['meme_score'] == 0].tolist())
    print('{}去除分数为0以及空字符串后meme个数为：{}'.format(filename, len(df)))

    df = df.sort_values(by='meme_score', ascending=False).reset_index(drop=True)
    print('按照分数递减排序后处理搭便车情况……')


    delete_set = set()
    delete_index_set = set()
    for index, row in df.iterrows():
        cur_nid = row['nid']
        cur_ngram = row['ngram']
        cur_score = row['meme_score']
        for i in range(index + 1, min(index + n + 1, len(df))):
            if cur_ngram in df.iloc[i]['ngram']:
                #             print('发现搭便车情况：nid：{}，ngram：{} --> nid：{}，ngram：{}'.format(cur_nid, cur_ngram, df.iloc[i]['nid'], df.iloc[i]['ngram']))
                delete_index_set.add(index)
                delete_set.add((cur_ngram, df.iloc[i]['ngram']))

            if df.iloc[i]['ngram'] in cur_ngram and isclose(df.iloc[i]['meme_score'], cur_score):
                #             print('-----')
                #             print('发现第二类搭便车情况：nid：{}，ngram：{} --> nid：{}，ngram：{}'.format(cur_nid, cur_ngram, df.iloc[i]['nid'], df.iloc[i]['ngram']))
                delete_index_set.add(i)
                delete_set.add((cur_ngram, df.iloc[i]['ngram']))

    df = df.drop(delete_index_set)
    print('{}处理搭便车情况后meme个数为：{}'.format(filename, len(df)))

    path = '../results/meme/processed/{}_processed.csv'.format(filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    print('正在将{}的处理结果写入csv文件……'.format(filename))
    df.to_csv(path, index=False)
    print('写入完成')

    path = '../results/meme/delete_situation/{}_deleted.txt'.format(filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    print('正在将{}的搭便车情况写入文本文件……'.format(filename))
    with open(path, 'w') as file:
        for i in delete_set:
            file.write('{},{}\n'.format(i[0],i[1]))
    print('写入完成')
