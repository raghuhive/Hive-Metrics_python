'''


@author:  rmurugaiyan
'''

import json
import requests
import pandas as pd
import numpy as np
import time
import os
import configparser
import grp
import pymysql
import getpass
import subprocess
#import paramiko
#from paramiko import SSHClient
from requests_kerberos import HTTPKerberosAuth
from os.path import expanduser
#from sshtunnel import SSHTunnelForwarder
from schemas import job_list
from factory import Factory
from utils import Setting
from sqlalchemy import create_engine
from sqlalchemy import text
import datetime as dt
from sqlalchemy.types import VARCHAR
import socket

###############################################################

def convert_json2pandas(jsn):    
    #jsn_l = json.loads(str(jsn, 'utf-8'))
    jsn_l = json.loads(str(jsn))
    data_jsn = jsn_l["apps"]
    data_apps_jsn = data_jsn["app"]
    df = pd.DataFrame(data_apps_jsn)
    return df    


def convert_json2pandas_databases(jsn):
    #jsn_l = json.loads(str(jsn, 'utf-8'))
    jsn_l = json.loads(str(jsn))
    data_jsn = jsn_l["databases"]
    df = pd.DataFrame(data_jsn)
    return df

    
###############################################################    
def get_res_mgr_df(config, loader):
    resource_manager_json = loader.get_resource_manager_api(config)
    return convert_json2pandas(resource_manager_json)

###############################################################  

'''
def load_mongo_db(res_mgr_df_processed, loader, config):
    mongo_client_db = loader.get_mongo_client(config)
    data = res_mgr_df_processed.to_dict(orient='records')
    mongo_client_db.col.remove()    
    mongo_client_db.col.insert_many(data)
    print (mongo_client_db.col.count())
    cursor = mongo_client_db.col.find()
    #for document in cursor:
    #    pprint(document)
'''  
  
###############################################################  
  

def process_res_mgr_df(res_mgr_df):   
    res_mgr_df = res_mgr_df[job_list.get_columns()] 
    #res_mgr_df = res_mgr_df[res_mgr_df['state'] == 'RUNNING']
    #print (res_mgr_df)
    print ("time is " + str(int(time.time())))
    res_mgr_df['creation_date']  = int(time.time())
    #print (res_mgr_df)
    print ("total count = " + str(res_mgr_df.count()))
    return res_mgr_df
          
############################################################### 
   
def process_resource_manager_api(config, loader):
    res_mgr_df = get_res_mgr_df(config, loader)
    return process_res_mgr_df(res_mgr_df)

        
############################################################### 

def process_webhcat_api(config, loader):
    databases = loader.get_webhcat_database_api(config)
    df= convert_json2pandas_databases(databases)
    databases_list=json.loads(databases)["databases"]
    print (databases_list)
    print (len(databases_list))
    '''
    for i in databases_list:
        tables = loader.get_webhcat_table_api(config)
        #print (tables) 
    '''
    #print ("Inside process_webhcat_api")

############################################################### 

def get_group_roles(config):
    print ("In get_group_roles")
    grps = config.getSetting('parameters', 'groups')
    grps = grps.replace(' ','')
    grps_arr = grps.split(',')
    d = {}
    for g in grps_arr:
        groups = grp.getgrnam(g)
        print (groups)
        d[groups[0]] = groups[3]
    print (d)
    #df = pd.DataFrame.from_dict(d, orient='index')
    #df = df.unstack().dropna().sort_index(level=1) 
    #df = df.unstack().dropna() 
    #print (df )


def get_local_port_from_host(config):
    mysql_host = config.getSetting('database', 'mysql_host')
    mysql_port = config.getSetting('database', 'mysql_port')
    user_name = getpass.getuser()
    user_host = user_name+'_'+mysql_host
    local_port=0
    for i in range(0,len(user_host)):
        local_port+=ord((user_host[i]))
    return int(local_port)+ int(mysql_port) 

    


def setup_tunnel_server(config,df):
    home = expanduser('~')
 

    local_port_num = get_local_port_from_host(config)
    tunnel_host = config.getSetting('database', 'tunnel_host')
    mysql_port = config.getSetting('database', 'mysql_port')
    mysql_host = config.getSetting('database', 'mysql_host')
    mysql_host2 = config.getSetting('database', 'mysql_host2')
    mysql_user2 = config.getSetting('database', 'mysql_user2')
    mysql_password2 = config.getSetting('database', 'mysql_password2')
    mysql_db = config.getSetting('database', 'mysql_db')
    mysql_port2 = config.getSetting('database', 'mysql_port2')

    # Logic to find whether the local port is opened or not
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((str(mysql_host2),local_port_num))
    if result == 0:
       print "Port is open"
    else:
       print "Port is not open"
       #Execute the Tunnel command
       ssh_command = 'ssh -f ' + tunnel_host + ' -o BatchMode=yes -o ConnectTimeout=60 -L ' + str(local_port_num) + ':' + str(mysql_host) + ':' + str(mysql_port) + ' -N'
       print (ssh_command)
       os.system(ssh_command)

    
    conn = pymysql.connect(host=str(mysql_host2), user=str(mysql_user2),
            passwd=str(mysql_password2), db=str(mysql_db),
            port=local_port_num)

    # Creating SQLALCHEMY Engine for Loading data into Mysql Tables  
    engine = create_engine("mysql+pymysql://"+str(mysql_user2)+":"+str(mysql_password2)+"@"+str(mysql_host2)+":"+str(local_port_num)+"/"+str(mysql_db))
    con = engine.connect()

    # READ MYSQL and load the data into DF and compare with current run dF
    #truncate_query = text("TRUNCATE TABLE hive_yarn_apps")
    #con.execution_options(autocommit=True).execute(truncate_query)

    #"""
    stmt="show tables like 'hive_yarn_log'"
    cursor = conn.cursor()
    cursor.execute(stmt)
    result = cursor.fetchone()
    print(df.count())
    print("the table is -  ",result)
    #df.index = pd.Series([dt.datetime.now()] * len(df))
    if result:
       # there is a table named "tableName"
          df.to_sql(name='hive_yarn_log_temp',con=con,if_exists='replace')
          con.close()
          sql = """
              delete from hive_yarn_log where id in (select id from hive_yarn_log_temp);
              insert into hive_yarn_log select * from hive_yarn_log_temp;        
          """
          with engine.begin() as con1:
               con1.execute(sql)
          con1.close()
           #"""
    else:
          #This step will run for the first time when the table is not available 
          print(df.columns)
          df.to_sql(name='hive_yarn_log',con=con,if_exists='replace')
    

if __name__ == '__main__':
    #pd.set_option('display.height', 1000)
    #pd.set_option('display.max_rows', 500)
    pd.set_option('display.width', 1000)
    pd.set_option('max_columns',10)
    
    config = Setting()
    f = Factory()
    loader = f.build('server') 
    
    #process resource manager URL
    res_mgr_df_processed = process_resource_manager_api(config, loader)
   
    setup_tunnel_server(config, res_mgr_df_processed)


