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
#import getent
import grp
import pymysql
import getpass
import subprocess
from pyhive import hive
from requests_kerberos import HTTPKerberosAuth
from os.path import expanduser
from schemas import job_list
from factory import Factory
from utils import Setting
from sqlalchemy import create_engine
from sqlalchemy import text
import socket

###############################################################


def convert_json2pandas_databases(jsn):
    jsn_l = json.loads(str(jsn))
    data_jsn = jsn_l 
    df = pd.DataFrame(data_jsn)
    return df

###############################################################
def convert_json2pandas_table(jsn):
    jsn_l = json.loads(str(jsn))
    data_jsn = jsn_l
    df = pd.DataFrame(data_jsn)
    return df

###################################################################
def convert_json2pandas_schema(jsn):
    jsn_l = json.loads(str(jsn))
    data_jsn = jsn_l["columns"]
    df = pd.DataFrame(data_jsn)
    return df
    
###############################################################    
def get_res_mgr_df(config, loader):
    resource_manager_json = loader.get_resource_manager_api(config)
    return convert_json2pandas(resource_manager_json)

###############################################################  
  
###############################################################  
   
def process_resource_manager_api(config, loader):
    res_mgr_df = get_res_mgr_df(config, loader)
    return process_res_mgr_df(res_mgr_df)

        
############################################################### 

def process_webhcat_api(config, loader):
    databases = loader.get_webhcat_database_api(config)
    if databases.status_code == 200:
       databases_content=databases.content
       databases_list=json.loads(databases_content)["databases"]
       appended_data=pd.DataFrame()
       for i in databases_list:
                     tables = loader.get_webhcat_table_api(config,i)
                     if tables.status_code == 200:
                             tables_content=tables.content
                             df2 = convert_json2pandas_table(tables_content)
                             df3 = df2.astype(str)
                             df_merged=pd.concat([df3, appended_data], ignore_index=True)
                             appended_data=df_merged
    return appended_data

############################################################### 

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
    #Get the config details from Config file
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

    
    engine = create_engine('mysql+pymysql://'+mysql_user2+':'+mysql_password2+'@'+str(mysql_host2)+':' + str(local_port_num) + '/'+mysql_db)
    
    con = engine.connect()
   
    df.to_sql(name='hive_hive_tables',con=con,if_exists='replace')
    con.close()
   
    query =  'select count(*) from hive_hive_tables;'
    data = pd.read_sql_query(query, conn)
    print (data)
    query = "select `database`, count(*) from hive_hive_tables group by `database`" 
    data = pd.read_sql_query(query, conn)
    print (data)
    conn.close()

if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    pd.set_option('max_columns',10)
    
    config = Setting()
    f = Factory()
    loader = f.build('server') 
    
    df=process_webhcat_api(config,loader)    
    setup_tunnel_server(config,df)


