'''


@author:  rmurugaiyan
'''

###############################################################
# job_list class.  name/columns etc.,  If new columns are required, add to cols list
class job_list(object):
    name = "job_list"
    
    cols = []
    cols.append('id')
    cols.append('user')
    cols.append('name')
    cols.append('state')
    cols.append('elapsedTime')
    cols.append('applicationType')
    cols.append('queue')
    cols.append('queueUsagePercentage')
    cols.append('progress')
    cols.append('startedTime')
    cols.append('finishedTime')
    cols.append('trackingUrl')


    @classmethod
    def get_columns(self):
        return self.cols      
    
    @classmethod
    def get_name(self):
        return self.name    



###############################################################

class table_list(object):
    name = "table_list"
    
    cols = []
    cols.append('database')
    cols.append('table')


    @classmethod
    def get_columns(self):
        return self.cols      
    
    @classmethod
    def get_name(self):
        return self.name    

###############################################################

class jhist_list(object):
    name = "jhist_list"

    cols = []
    cols.append('submitTime')
    cols.append('startTime')
    cols.append('finishTime')
    cols.append('id')
    cols.append('name')
    cols.append('queue')
    cols.append('user')
    cols.append('state')
    cols.append('mapsTotal')
    cols.append('mapsCompleted')
    cols.append('reducesTotal')
    cols.append('reducesCompleted')

    @classmethod
    def get_columns(self):
        return self.cols

    @classmethod
    def get_name(self):
        return self.name

