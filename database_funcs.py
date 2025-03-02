
import mysql.connector
from config import load_config
import pandas as pd


def db_connect(filename):
    with open(f'_sensitive_/{filename}', "r") as f:
        details = f.read().split()
        #conn_string = ("host='%s' dbname='%s' user='%s' password='%s'",details[5],"music_db",details[1],details[3])
        conn = mysql.connector.connect(host=details[5],user=details[1],password=details[3],database = "music_db",use_pure=True, connection_timeout=None)
    return conn
def db_connect():
    cnfg = load_config()
    return mysql.connector.connect(**cnfg , use_pure=True)

def arr_to_str(arr):
    str = "("
    for item in arr:
        str += item + ", "
    return str[:-2] + ")"

def insert_to_db(db,insert_file,data):
    cursor = db.cursor()
    query = open("sql/"+insert_file,"r").readlines()
    cursor.execute(query,data)


print("connecting to db")
query = open("sql/"+"test_connection.sql","r").read()

db = db_connect()
print(query)
df = pd.read_sql_query(query, db)
print(df)