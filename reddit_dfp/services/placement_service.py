from googleads import dfp
from pylons import g

from reddit_dfp.lib.dfp import DfpService
from reddit_dfp.services import (
    advertisers_service,
)


def get_placement_by_name(name):
    dfp_placement_service = DfpService("PlacementService")

    values = [{
        "key": "name",
        "value": {
            "xsi_type": "TextValue",
            "value": name,
        },
    }]

    query = "WHERE name = :name"
    statement = dfp.FilterStatement(query, values, 1)

    response = dfp_placement_service.execute(
                "getPlacementsByStatement",
                statement.ToStatement())

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


