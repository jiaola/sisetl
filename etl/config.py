from dotenv import load_dotenv
import os

env = os.getenv('ENV', 'dev')
if env is None:
    load_dotenv(verbose=True)  # load the default .env file
else:
    load_dotenv(dotenv_path=f'.env.{env}', verbose=True)


class Config:
    MONGO_HOST = os.getenv('MONGODB_HOST', 'mongo')
    MONGO_PORT = int(os.getenv('MONGODB_PORT', '27017'))
    MONGO_DATABASE = os.getenv('MONGODB_DATABASE', 'scopus')


class DevelopmentConfig(Config):
    MONGO_DATABASE = os.getenv('MONGODB_DATABASE', 'biophysics')


class TestConfig(Config):
    MONGO_DATABASE = os.getenv('MONGODB_DATABASE', 'test')


class ProductionConfig(Config):
    MONGO_DATABASE = os.getenv('MONGODB_DATABASE', 'scopus')


if env == 'dev':
    config = DevelopmentConfig
elif env == 'test':
    config = TestConfig
elif env == 'prod':
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
api_limits = {
    'serial': 20000,
    'abstract': 10000,
    'affiliation': 5000,
    'author': 5000,
    'author_search': 5000,
    'scopus_search': 20000,
    'affiliation_search': 5000,
    'serial_title': 20000,
}
api_keys = os.getenv('SCOPUS_API_KEYS', '').split(':')
key_manager = KeyManager(limits=api_limits, keys=api_keys)
