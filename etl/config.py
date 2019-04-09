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
