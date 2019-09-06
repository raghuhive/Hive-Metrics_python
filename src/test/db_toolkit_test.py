'''

@author: rmurugaiyan1
'''

import unittest
from main.utils import Setting
from main.factory import Factory
from main import db_toolkit as tk

class Test(unittest.TestCase):
    def test_db_toolkit(self):
        config = Setting()
        f = Factory()
        print ("Here")
        loader = f.build('local')
        df = tk.process_resource_manager_api(config, loader)
        self.assertEquals(df['id'].count(), 247)

