from googleads import dfp
from pylons import g

from reddit_dfp.lib.dfp import DfpService
from reddit_dfp.services import (
    advertisers_service,
)


def get_template_by_name(name):
    dfp_template_service = DfpService("CreativeTemplateService")

    values = [{
        "key": "name",
        "value": {
            "xsi_type": "TextValue",
            "value": name,
        },
    }]

    query = "WHERE name = :name"
    statement = dfp.FilterStatement(query, values, 1)

    response = dfp_template_service.execute(
                "getCreativeTemplatesByStatement",
                statement.ToStatement())

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


