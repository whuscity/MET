import pandas as pd
import os

START_YEAR = 2000
END_YEAR = 2016
SPAN = 5
df_list = []
i = 1

for year in range(START_YEAR, END_YEAR - SPAN + 2):
    print('正在处理{}-{}的数据'.format(year, year + SPAN - 1))
    df = pd.read_csv(
        r'C:\Users\Tom\PycharmProjects\MET\results\energy\k_2\{}-{}.csv'.format(str(year), str(year + SPAN - 1)))
    print('原始长度：', len(df))
    df = df.drop(df[(df.latitude == 999) | (df.ipc_triangles == -1)].index)
    print('过滤后长度：', len(df))
    df = df.assign(year=i)
    i += 1

    df_list.append(df)

full_df = pd.concat(df_list).drop_duplicates()

filename = '../results/energy/processed/all_year_processed.csv'
os.makedirs(os.path.dirname(filename), exist_ok=True)
full_df.to_csv(filename, index=False, quoting=1)