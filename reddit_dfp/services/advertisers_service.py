import time

from googleads import dfp
from suds import WebFault

from reddit_dfp.lib.dfp import DfpService

MAX_RETRIES = 3


def get_advertiser(user):
    advertiser_id = getattr(user, "dfp_advertiser_id", None)

    if not advertiser_id:
        return None

    values = [{
        "key": "id",
        "value": {
            "xsi_type": "NumberValue",
            "value": advertiser_id,
        },
    }]

    dfp_company_service = DfpService("CompanyService")
    statement = dfp.FilterStatement("WHERE id = :id", values, 1).ToStatement()
    response = dfp_company_service.execute("getCompaniesByStatement", statement)

    if "results" in response and len(response["results"]):
        return response["results"][0]
    else:
        return None


def create_advertiser(user):
    dfp_company_service = DfpService("CompanyService")
    companies = dfp_company_service.execute("createCompanies", [{
        "name": user.name,
        "type": "ADVERTISER",
        "externalId": user._fullname,
    }])

    advertiser = companies[0]

    user.dfp_advertiser_id = advertiser["id"]
    user._commit()

    return advertiser


def upsert_advertiser(user):
    advertiser = get_advertiser(user)

    if advertiser:
        return advertiser

    return create_advertiser(user)
