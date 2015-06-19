import time

from googleads import dfp
from suds import WebFault

from reddit_dfp.lib import errors
from reddit_dfp.lib.dfp import get_service

MAX_RETRIES = 3


def get_advertiser(user):
    dfp_company_service = get_service("CompanyService")
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

    query = "WHERE id = :id"
    statement = dfp.FilterStatement(query, values, 1)
    response = None
    retries = 0
    while response == None and retries < MAX_RETRIES:
        try:
            response = dfp_company_service.getCompaniesByStatement(
                    statement.ToStatement())
        except WebFault as e:
            if errors.get_reason(e) == "EXCEEDED_QUOTA":
                wait = 2 ** retries
                print "failed attempt %d, retrying in %d seconds." % ((retries + 1), wait)
                time.sleep(wait)
                retries += 1
            else:
                raise e

    if not response:
        return None

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


def create_advertiser(user):
    dfp_company_service = get_service("CompanyService")

    companies = [{
        "name": user.name,
        "type": "ADVERTISER",
        "externalId": user._fullname,
    }]

    companies = dfp_company_service.createCompanies(companies)

    advertiser = companies[0]

    user.dfp_advertiser_id = advertiser["id"]
    user._commit()

    return advertiser


def upsert_advertiser(user):
    advertiser = get_advertiser(user)

    if advertiser:
        return advertiser

    return create_advertiser(user)
