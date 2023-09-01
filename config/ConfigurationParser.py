import configparser


# TODO Work out a better way to do this
def get_config(k, v):
    config = configparser.ConfigParser()
    config.read("./configuration.cfg")
    return config.get(k, v)
