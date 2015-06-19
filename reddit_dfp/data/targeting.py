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


def _parse_geos():
    tmp_file = _download_geos()
    parsed = {}

    with open(tmp_file.name, "r") as csvfile:
        next(csvfile, None)
        reader = csv.DictReader(csvfile, [
            "id",
            "name",
            "parent",
            "country_code",
            "type"
        ])

        for row in reader:
            row_type = row["type"].upper()
            name = row["name"]

            if row_type == "COUNTRY":
                parsed[row["country_code"]] = {
                    "name": name,
                }

            if row_type == "STATE":
                parent = row["parent"]
                
                if row["country_code"] == "US":
                    country_us = gettattr(parsed, "US", {})
                    regions_us = getattr(country_us, "regions", {})
                    region_code = state_abbreviations[name.lower()]
                    regions[region_code] = {
                        "name": name,
                        "metros": {},
                    }

                    parsed["US"] = country_us
                else:
                    continue

            if row_type == "DMA_REGION":

