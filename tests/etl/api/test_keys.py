import unittest
from etl.config import key_manager, KeyManager


def mock_get_author():
    api_key = key_manager.get_key('author')
    return api_key


def mock_get_document():
    api_key = key_manager.get_key('abstract')
    return api_key


class KeyManagerTestCase(unittest.TestCase):

    def test_get_key(self):
        api_limits = {
            'serial': 1,
            'abstract': 2,
        }
        api_keys = ['key1', 'key2']
        km = KeyManager(limits=api_limits, keys=api_keys)
        value = km.get_key('serial')
        self.assertEqual(value, 'key1')
        value = km.get_key('serial')
        self.assertEqual(value, 'key2')
        value = km.get_key('serial')
        self.assertIsNone(value)
        value = km.get_key('abstract')
        self.assertEqual(value, 'key1')
        value = km.get_key('abstract')
        self.assertEqual(value, 'key1')
        value = km.get_key('abstract')
        self.assertEqual(value, 'key2')

    def test_config_key_manager(self):
        for n in range(0, 500000):
            key_author = mock_get_author()
            key_document = mock_get_document()
        self.assertIsNone(key_author)
        self.assertIsNone(key_document)


if __name__ == '__main__':
    unittest.main()
