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
import socket

###############################################################


  


def get_group_roles(config):
    print ("In get_group_roles")
    grps = config.getSetting('parameters', 'groups')
    grps = grps.replace(' ','')
    grps_arr = grps.split(',')
    print(grps_arr)
    d = {}
    print(type(d))
    for g in grps_arr:
        groups = grp.getgrnam(g)
        #print (type(groups))
        d[groups[0]] = groups[3]
    #df=pd.concat({k: pd.Series(v) for k, v in d.items()})
    #df=pd.Series(d).reset_index()
    #df.columns=['Groups','USER'] 
    df=pd.DataFrame([[k,v] for k,v in d.items()]).rename(columns={0:'Groups',1:'User'})
        
        #combined = dict(item for d in groups for item in d.items())
        #for value in combined.values():
        #    print(value)
    #print (d)
    #df = pd.DataFrame.from_dict(d, orient='index')
    #df=pd.DataFrame.transpose(
    #df = df.unstack().dropna().sort_index(level=1) 
    #df = df.unstack().dropna() 
    #df=df.T
    print (df)
    
    #df1 = df.User.apply(pd.Series).stack()
    #df2 = df1.to_frame().reset_index(1, drop=False)
    #df2.join(df.Groups).reset_index(drop=False)
    df1=explode(df.assign(var1=df.User.str.split(',')), 'User')
    del df1['var1']
    print(df1)
    return df1




    #return df


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

    engine = create_engine("mysql+pymysql://"+str(mysql_user2)+":"+str(mysql_password2)+"@"+str(mysql_host2)+":" + str(local_port_num)+"/"+str(mysql_db))
    con = engine.connect()
    df.to_sql(name='hive_user_groups',con=con, if_exists='replace')
    con.close()

    query =  'select * from hive_user_groups limit 10;'
    data = pd.read_sql_query(query, conn)
    print (data)
    query = 'select count(*) from hive_user_groups;'
    data = pd.read_sql_query(query, conn)
    print (data)
    conn.close()
    
def explode(df, lst_cols, fill_value=''):
    # make sure `lst_cols` is a list
    if lst_cols and not isinstance(lst_cols, list):
        lst_cols = [lst_cols]
    # all columns except `lst_cols`
    idx_cols = df.columns.difference(lst_cols)

    # calculate lengths of lists
    lens = df[lst_cols[0]].str.len()

    if (lens > 0).all():
        # ALL lists in cells aren't empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, lens)
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .loc[:, df.columns]
    else:
        # at least one list in cells is empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, lens)
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .append(df.loc[lens==0, idx_cols]).fillna(fill_value) \
          .loc[:, df.columns]

if __name__ == '__main__':
    #pd.set_option('display.height', 1000)
    #pd.set_option('display.max_rows', 500)
    pd.set_option('display.width', 1000)
    pd.set_option('max_columns',10)
    
    config = Setting()
    f = Factory()
    loader = f.build('server') 
    

    #get group info
    df=get_group_roles(config)
    
    setup_tunnel_server(config,df)


