import os


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
            if count < self.limits[api]:
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
}
api_keys = os.getenv('SCOPUS_API_KEYS', '').split(':')
key_manager = KeyManager(limits=api_limits, keys=api_keys)
