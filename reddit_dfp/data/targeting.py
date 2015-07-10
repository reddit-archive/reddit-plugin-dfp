import csv
import tempfile

from googleads import dfp
from pylons import g

from reddit_dfp.data.states import state_abbreviations
from reddit_dfp.lib.dfp import get_downloader


def _download_geos():
    dfp_report_downloader = get_downloader()
    tmp_geos = tempfile.NamedTemporaryFile(
            prefix="geo_targets_", suffix=".csv", mode="w", delete=False)

    pql_query = ("SELECT Id, Name, CanonicalParentId, CountryCode, Type "
                 "FROM Geo_Target "
                 "WHERE targetable = true AND "
                 "type = 'Country' OR "
                 "(CountryCode = 'US' AND type IN ('State', 'DMA_Region'))")

    dfp_report_downloader.DownloadPqlResultToCsv(
            pql_query, output_file)
    output_file.close()

    print ("Saved geo targets to... %s" % output_file.name)

    return output_file


def _download_mobile_os():
    dfp_report_downloader = get_downloader()
    output_file = tempfile.NamedTemporaryFile(
            prefix="os_targets_", suffix=".csv", mode="w", delete=False)

    pql_query = "SELECT Id, OperatingSystemName FROM Operating_System"

    dfp_report_downloader.DownloadPqlResultToCsv(
            pql_query, output_file)
    output_file.close()

    print ("Saved mobile os targets to... %s" % output_file.name)

    return output_file

