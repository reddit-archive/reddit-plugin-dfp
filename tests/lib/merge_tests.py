import unittest

from reddit_dfp.lib import merge


class MergeTests(unittest.TestCase):
    def test_merge(self):
        merged = merge.merge({
            "foo": "bar",
        }, {
            "boo": "far",
        })

        self.assertEqual(merged, {
            "foo": "bar",
            "boo": "far",
        })

    def test_merge_multiple_dicts(self):
        merged = merge.merge({
            "foo": "bar",
        }, {
            "boo": "far",
        }, {
            "bat": "bing",
        })

        self.assertEqual(merged, {
            "foo": "bar",
            "boo": "far",
            "bat": "bing",
        })

    def test_merge_overwrites_existing(self):
        merged = merge.merge({
            "foo": "bar",
        }, {
            "foo": "far",
        })

        self.assertEqual(merged, {
            "foo": "far",
        })

    def test_merge_deep(self):
        merged = merge.merge_deep({
            "foo": {
                "a": 1,
                "b": 2,
            },
        }, {
            "foo": {
                "c": 3,
            },
        })

        self.assertEqual(merged, {
            "foo": {
                "a": 1,
                "b": 2,
                "c": 3,
            },
        })
