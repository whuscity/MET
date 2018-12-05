import configparser
import pymysql

db_config = configparser.ConfigParser()
db_config.read(r'..\conf\db_connection_detail.ini')



def get_db_connection(config):
    DB_USER = db_config.get(config, 'db_user')
    DB_USER_PASSWORD = db_config.get(config, 'db_user_password')
    DB_HOST = db_config.get(config, 'db_host')
    DB_PORT = db_config.getint(config, 'db_port')
    DB_NAME = db_config.get(config, 'db_name')
    con = pymysql.connect(DB_HOST, DB_USER, DB_USER_PASSWORD, DB_NAME, DB_PORT)
    con.autocommit(True)
    # cursor = con.cursor()
    # con.close()
    print('获取数据库连接成功')
    return con

if __name__ == '__main__':
    get_db_connection('MET')
