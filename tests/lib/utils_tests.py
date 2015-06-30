import unittest
import mock

from datetime import datetime, date

from reddit_dfp.lib import utils


class UtilsTests(unittest.TestCase):
    def test_datetime_to_dfp_datetime_w_date(self):
        year = 2015
        month = 1
        day = 1

        dfp_datetime = utils.datetime_to_dfp_datetime(date(year, month, day))

        self.assertIsInstance(dfp_datetime, dict)
        self.assertEqual(dfp_datetime["date"]["year"], year)
        self.assertEqual(dfp_datetime["date"]["month"], month)
        self.assertEqual(dfp_datetime["date"]["day"], day)

    def test_datetime_to_dfp_datetime_w_datetime(self):
        year = 2015
        month = 1
        day = 1
        hour = 11
        minute = 0
        second = 0

        dfp_datetime = utils.datetime_to_dfp_datetime(datetime(year, month, day, hour, minute, second))

        self.assertIsInstance(dfp_datetime, dict)
        self.assertEqual(dfp_datetime["date"]["year"], year)
        self.assertEqual(dfp_datetime["date"]["month"], month)
        self.assertEqual(dfp_datetime["date"]["day"], day)
        self.assertEqual(dfp_datetime["hour"], hour)
        self.assertEqual(dfp_datetime["minute"], minute)
        self.assertEqual(dfp_datetime["second"], second)

