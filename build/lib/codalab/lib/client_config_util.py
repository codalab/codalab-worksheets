import json
import sys, os
from codalab.lib import path_util
from codalab.config.config_parser import ConfigParser
from os.path import expanduser, abspath, exists


def Home():
    target_path = expanduser("~")
    home_path = os.path.join(target_path,".codalab")
    return home_path

def Normalize(path, file_name):
    file_path = os.path.join(path, file_name)
    return file_path

def ReadFile(target_file):
    target = open(target_file, 'r')
    result = json.load(target)
    return result

def CurrentHost(config_file):
    '''
    A function that returns the current host.
    '''
    tmp = config_file['client']['class']
    return tmp

def UpdateHost(config_file, target_host):
    '''
    Function that updates the current client
    host to any specified client host.
    '''
    tmp = config_file['client']['class']
    config_file['client']['class'] = target_host
    jsonfile = open(file_access, 'w')
    jsonfile.write(json.dumps(config_file))
    jsonfile.close()
    return "Host updated"

def UpdateVerbosity(config_file, target_verbosity):
    tmp = config_file['cli']['verbose']
    config_file['cli']['verbose'] = target_verbosity
    jsonfile = open(file_access, 'w')
    jsonfile.write(json.dumps(config_file))
    jsonfile.close()
    return "Verbosity updated"


home = Home()
file_access = Normalize(home, "client_config.json")
config_file = ReadFile(file_access)
