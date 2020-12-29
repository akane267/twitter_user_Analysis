import configparser

ConfigPath = './config.ini'


def GetTwitterAPIkey():
    global ConfigPath
    config_ini = configparser.ConfigParser()
    config_ini.read(ConfigPath)
    twitter_api_key = {'Consumer_key': config_ini['TwitterAPI']['Consumer_key'],
                       'Consumer_secret': config_ini['TwitterAPI']['Consumer_secret'],
                       'Access_token': config_ini['TwitterAPI']['Access_token'],
                       'Access_secret': config_ini['TwitterAPI']['Access_secret']
                       }
    return twitter_api_key


def GetGCPAPIPath():
    global ConfigPath
    config_ini = configparser.ConfigParser()
    config_ini.read(ConfigPath)
    return config_ini['GCPAPI']['Json_Path']


def main():
    return


if __name__ == '__main__':
    main()
    GetTwitterAPIkey()
