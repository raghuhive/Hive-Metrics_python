'''


@author:  rmurugaiyan
'''

from dataloader import TestDataLoader
from dataloader import DataLoader

class Factory():
    def build (self, environ):
        if (environ == 'local'):
            return TestDataLoader()
        if (environ == 'server'):
            return DataLoader()
        
        
