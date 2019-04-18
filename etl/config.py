import os
import yaml

__env__ = os.getenv('ENV', 'dev')

with open(f'config.{__env__}.yml', 'r') as stream:
    try:
        config_yaml = yaml.safe_load(stream)
    except yaml.YAMLError as ex:
        print(ex)


class Config:
    MONGO_HOST = config_yaml.get('mongodb', {}).get('host', 'localhost')
    MONGO_PORT = config_yaml.get('mongodb', {}).get('port', 27017)
    MONGO_DATABASE = config_yaml.get('mongodb', {}).get('database', 'sisbib')


class DevelopmentConfig(Config):
    MONGO_DATABASE = config_yaml.get('mongodb', {}).get('database', 'biophysics')


class TestConfig(Config):
    MONGO_DATABASE = config_yaml.get('mongodb', {}).get('database', 'test')


class ProductionConfig(Config):
    MONGO_DATABASE = config_yaml.get('mongodb', {}).get('database', 'sisbib')


if __env__ == 'dev':
    config = DevelopmentConfig
elif __env__ == 'test':
    config = TestConfig
elif __env__ == 'prod':
    config = ProductionConfig
else:
    raise ValueError('Invalid environment name. It has to be one of dev, test or prod.')


class KeyManager:
    """
    A manager of API keys. It returns a key without exceeding the quota.
    """
    def __init__(self, keys=[], limits={}):
        self.keys = keys
        self.limits = limits
        # initialize counters
        self.counters = {}
        for key in keys:
            self.counters[key] = dict((el, 0) for el in limits)

    def get_key(self, api):
        for key in self.counters:
            count = self.counters[key][api]
            if count < self.limits[api] - 200:  # use a buffer. The keys might have been used in other cases
                self.counters[key][api] = count + 1
                return key
        return None


# See https://dev.elsevier.com/api_key_settings.html
__api_limits__ = {
    'serial': 20000,
    'abstract': 10000,
    'affiliation': 5000,
    'author': 5000,
    'author_search': 5000,
    'scopus_search': 20000,
    'affiliation_search': 5000,
    'serial_title': 20000,
}
__api_keys__ = config.get('scopus', {}).get('api_keys', [])
key_manager = KeyManager(limits=__api_limits__, keys=__api_keys__)
