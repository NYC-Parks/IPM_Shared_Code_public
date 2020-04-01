import configparser

def get_config(cfile):
    # config = ConfigParser.ConfigParser()
    config = configparser.ConfigParser()
    config.read(cfile)
    sections = config.sections()
    config_dict = {}
    for section in sections:
        config_dict[section] = {}
        for option in config.options(section):
            config_dict[section][option] = config.get(section,option)
    return config_dict
