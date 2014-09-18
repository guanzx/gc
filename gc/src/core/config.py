#encoding=utf-8

import ConfigParser

class Config(object):
    
    def __init__(self,confDir,type):   
        self._confDir = confDir
        self._type = type
            
    def getConfigDir(self):
        """ 得到配置路径. """
        config = ConfigParser.ConfigParser()
        config.readfp(open(self._confDir))
        dir_options = config.options(self._type)
        return dir_options,config