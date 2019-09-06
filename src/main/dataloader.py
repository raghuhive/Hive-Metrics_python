'''

@author:  rmurugaiyan
'''

import requests
from requests_kerberos import HTTPKerberosAuth
#from pymongo import MongoClient
import os

class TestDataLoader():
    def get_resource_manager_api(self, config):
        print ("In test get_resource_manager_api ")
        json_file_path = os.path.join(os.path.dirname(__file__), os.pardir, 'test/resource_manager.json')
        resource_manager_list = open(json_file_path)
        return resource_manager_list.readline()
    
    def get_mongo_client(self, config):
        print("In test get_mongo_client")
        #client = MongoClient(port=<port-no>)
        #col = client['test']['test']
        #return client.mydb
    
    def get_webhcat_database_api(self, config):
        #print ("In get_webhcat_database_api ")
        return resource.content

            
class DataLoader():
    def get_resource_manager_api(self, config):
        #print ("In get_resource_manager_api ")
        url = config.getSetting('parameters', 'resource_manager_url')
        resource = requests.get(url)
        return resource.content

    def get_mongo_client(self, config):
        print("In get_mongo_client")


    def get_webhcat_database_api(self, config):
        url = config.getSetting('parameters', 'webhcat_horizon_url')
        resource = requests.get(url,auth=HTTPKerberosAuth())
        return resource


    def get_webhcat_table_api(self, config,i):
        url = config.getSetting('parameters', 'webhcat_horizon_url')
        tbl_url = url + "/" + i + "/table"
        resource = requests.get(tbl_url,auth=HTTPKerberosAuth())
        return resource

    
    def get_webhcat_schema_api(self, config,i,j):
        url = config.getSetting('parameters', 'webhcat_horizon_url')
        url = url + "/" + i + "/table" + "/" + j + "?format=extended"
        print(url)
        resource = requests.get(url,auth=HTTPKerberosAuth())
        return resource
