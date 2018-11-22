import sqlite3
from itertools import combinations


def generate_ipc_combinations(con, fromtable, totable):
    query_sql = 'SELECT `patnum`, `ipc_classes`, `year`, `city`, `country` FROM {} ORDER BY `patnum`'.format(fromtable)
    insert_sql = 'INSERT INTO {} VALUES (?, ?, ?, ?, ?, ?)'.format(totable)
    insert_list = []

    cursor = con.cursor()
    cursor.execute(query_sql)
    result = cursor.fetchall()

    for row in result:
        combines = list(combinations(row[1].split(','), 2))
        for ipc_pairs in combines:
            insert_list.append((row[0], ipc_pairs[0], ipc_pairs[1], row[2], row[3], row[4]))

    cursor.executemany(insert_sql, insert_list)
    con.commit()

def generate_city_combinations(con, fromtable, totable):
    query_sql = 'SELECT `patnum`, `cities`, `year`, `country` FROM {} ORDER BY `patnum`'.format(fromtable)
    insert_sql = 'INSERT INTO {} VALUES (?, ?, ?, ?, ?)'.format(totable)
    insert_list = []

    cursor = con.cursor()
    cursor.execute(query_sql)
    result = cursor.fetchall()

    for row in result:
        combines = list(combinations(row[1].split(','), 2))
        for city_pairs in combines:
            insert_list.append((row[0], city_pairs[0], city_pairs[1], row[2], row[3]))

    cursor.executemany(insert_sql, insert_list)
    con.commit()


if __name__ == '__main__':
    connection = sqlite3.connect(r'C:\Users\Tom\Documents\energy.db')
    # generate_ipc_combinations(connection, 'energy_ipc_wait_for_combination', 'energy_ipc_cooccurrence')
    generate_city_combinations(connection, 'energy_city_wait_for_combination', 'energy_city_cooccurrence')
    connection.close()
