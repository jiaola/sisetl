import unittest
from etl.api.keys import KeyManager


class KeyManagerTestCase(unittest.TestCase):
    def test_get_key(self):
        api_limits = {
            'serial': 1,
            'abstract': 2,
        }
        api_keys = ['key1', 'key2']
        key_manager = KeyManager(limits=api_limits, keys=api_keys)
        value = key_manager.get_key('serial')
        self.assertEqual(value, 'key1')
        value = key_manager.get_key('serial')
        self.assertEqual(value, 'key2')
        value = key_manager.get_key('serial')
        self.assertIsNone(value)
        value = key_manager.get_key('abstract')
        self.assertEqual(value, 'key1')
        value = key_manager.get_key('abstract')
        self.assertEqual(value, 'key1')
        value = key_manager.get_key('abstract')
        self.assertEqual(value, 'key2')


if __name__ == '__main__':
    unittest.main()
