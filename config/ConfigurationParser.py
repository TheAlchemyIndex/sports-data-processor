import configparser


# TODO Work out a better way to do this
def get_config(k, v):
    config = configparser.ConfigParser()
    config.read("C:/repos/sports-data-processor/configuration.cfg")
    return config.get(k, v)
