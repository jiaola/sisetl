from bonobo.config import use, ContextProcessor, Configurable, Option, Service
from bonobo.util.objects import ValueHolder
from bonobo.constants import NOT_MODIFIED


def fix_keys(o):
    """
    There's an issue if the key starts with '$' when inserting into mongo.
    This function replaces the starting '$' with '_' in any key of an object
    :param o: the object
    :return: an object with all keys fixed
    """
    if isinstance(o, list):
        return [fix_keys(x) for x in o]
    elif isinstance(o, dict):
        d = {}
        for k, v in o.items():
            new_k = '_' + k[1:] if k[0] == '$' else k
            d[new_k] = fix_keys(v)
        return d
    else:
        return o


def extract_id(args):
    return args['dc:identifier'].split(':')[1]


def remove_errata(doc):
    if doc['coredata']['subtype'] != 'er': # not erreta
        yield NOT_MODIFIED


class Uniquify(Configurable):
    """Deduplicate rows based on a field"""
    field = Option(positional=True, default=0)

    @ContextProcessor
    def unique_set(self, context):
        yield ValueHolder(set())

    def __call__(self, unique_set, *args, **kwargs):
        if args[self.field] not in unique_set:
            unique_set.add(args[self.field])
            yield NOT_MODIFIED


class FilterDuplicate(Configurable):
    field = Option(positional=True, default=0)
    target = Option(positional=True, default='_id')
    database = Option(str, positional=True, default='scopus', __doc__='the mongo database')
    collection = Option(str, positional=True, default='', __doc__='the mongo collection')
    client = Service('mongodb.client')

    def __call__(self, args, *, client):
        db = client[self.database]
        collection = db[self.collection]
        if isinstance(args, dict) or isinstance(args, list):
            if collection.find_one({self.target: args[self.field]}) is None:
                yield NOT_MODIFIED
        else:
            if collection.find_one({self.target: args}) is None:
                yield NOT_MODIFIED


class FilterSerialTitle(Configurable):
    """
    Check if a title already exists
    """
    database = Option(str, positional=True, default='scopus', __doc__='the mongo database')
    collection = Option(str, positional=True, default='serial', __doc__='the mongo collection')
    client = Service('mongodb.client')

    def __call__(self, title, count, *, client):
        db = client[self.database]
        collection = db[self.collection]
        if collection.find_one({'dc:title': {'$in': [title, 'The '+title]}}) is None:
            yield NOT_MODIFIED


class MongoWriter(Configurable):
    database = Option(str, positional=True, default='scopus', __doc__='the mongodb database name')
    collection = Option(str, positional=True, default='', __doc__='the mongodb collection name')
    client = Service('mongodb.client')

    def __call__(self, args, *, client):
        db = client[self.database]
        collection = db[self.collection]
        collection.insert_one(fix_keys(args))


class MongoReader(Configurable):
    database = Option(str, positional=True, default='scopus', __doc__='the mongodb database name')
    collection = Option(str, positional=True, default='', __doc__='the mongodb collection name')
    client = Service('mongodb.client')

    def __call__(self, args, *, client):
        db = client[self.database]
        collection = db[self.collection]
