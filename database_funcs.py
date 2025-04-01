
import mysql.connector
from config import load_config
import pandas as pd
import hashlib

def getID(value):
    hashObj = hashlib.sha256()
    hashObj.update(value.encode('utf-8'))
    hexHash= hashObj.hexdigest()
    ID = int(hexHash,16)%2147483647
    return ID

def db_connect(filename):
    with open(f'_sensitive_/{filename}', "r") as f:
        details = f.read().split()
        #conn_string = ("host='%s' dbname='%s' user='%s' password='%s'",details[5],"music_db",details[1],details[3])
        conn = mysql.connector.connect(host=details[5],user=details[1],password=details[3],database = "music_db",use_pure=True, connection_timeout=None)
    return conn

def db_connect():
    cnfg = load_config()
    return mysql.connector.connect(**cnfg , use_pure=True)

def load_query(filename):
    return open("sql/" + filename,'r').read()

if __name__ == "__main__":
    db = db_connect()
    print(pd.read_sql_query(load_query("test_connection.sql"),db))