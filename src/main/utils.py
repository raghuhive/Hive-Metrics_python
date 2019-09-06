'''


@author:  rmurugaiyan
'''
import configparser
import os

configFilePath = os.path.join(os.path.dirname(__file__), os.pardir, 'resource/config.cfg')

class Setting(object):
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        print(configFilePath)
        self.cfg.read(configFilePath) 
    
    def getSetting(self, section, mysetting):
        return self.cfg.get(section, mysetting)    

