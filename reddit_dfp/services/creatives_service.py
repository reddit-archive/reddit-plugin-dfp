from googleads import dfp
from pylons import g

from r2.lib.utils import to36
from r2.models import (
    Account,
    Link,
    promo,
)

from reddit_dfp.lib import utils
from reddit_dfp.lib.dfp import DfpService
from reddit_dfp.lib.merge import merge_deep
from reddit_dfp.services import (
    lineitems_service,
    advertisers_service,
)

NATIVE_SIZE = {
    "width": "1",
    "height": "1",
}


def _get_creative_name(link):
    return "%s [%s]" % (utils.trim(link.title, 150), utils.trim(link.url, 100))


def _link_to_creative(link, advertiser=None, existing=None):
    defaults = {
        "xsi_type": "TemplateCreative",
        "size": NATIVE_SIZE,
        "creativeTemplateId": g.dfp_selfserve_template_id,
    }

    if not (existing or advertiser):
        raise ValueError("must either pass an advertiser or an existing creative.")

    creative = {
        "name": _get_creative_name(link),
        "creativeTemplateVariableValues": [{
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "title",
            "value": link.title
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "url",
            "value": link.url,
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "selftext",
            "value": link.selftext
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "thumbnail_url",
            "value": getattr(link, "thumbnail_url", ""),
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "mobile_ad_url",
            "value": getattr(link, "mobile_ad_url", ""),
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "third_party_tracking",
            "value": getattr(link, "third_party_tracking", ""),
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "third_party_tracking_2",
            "value": getattr(link, "third_party_tracking_2", ""),
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "link_id",
            "value": link._fullname,
        }],
    }

    if existing:
        return merge_deep(existing, creative)
    else:
        return merge_deep(creative, defaults, {
            "advertiserId": advertiser.id,
        })


def get_creative(link):
    creative_id = getattr(link, "dfp_creative_id", None)

    if not creative_id:
        return None

    return by_id(creative_id)


def by_id(creative_id):
    dfp_creative_service = DfpService("CreativeService")

    values = [{
        "key": "id",
        "value": {
            "xsi_type": "NumberValue",
            "value": creative_id,
        },
    }]

    query = "WHERE id = :id"
    statement = dfp.FilterStatement(query, values, 1).ToStatement()
    response = dfp_creative_service.execute("getCreativesByStatement", statement)

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


def create_creative(user, link):
    dfp_creative_service = DfpService("CreativeService")
    advertiser = advertisers_service.upsert_advertiser(user)
    creative = _link_to_creative(link, advertiser=advertiser)

    response = dfp_creative_service.execute("createCreatives", [creative])

    if (response and len(response)):
        creative = response[0]
        link.dfp_creative_id = creative.id
        link._commit()

        return creative
    else:
        return None

def upsert_creative(user, link):
    creative = get_creative(link)

    if not creative:
        return create_creative(user, link)

    return update_creative(link, creative)


def update_creative(link, creative):
    dfp_creative_service = DfpService("CreativeService")
    updated = _link_to_creative(link, existing=creative)
    creatives = dfp_creative_service.execute("updateCreatives", [updated])

    return creatives[0]


def bulk_upsert(links):
    updates = filter(lambda user: getattr(user, "dfp_creative_id", False), links)
    inserts = filter(lambda user: not getattr(user, "dfp_creative_id", False), links)

    dfp_creative_service = DfpService("CreativeService")
    creatives = []

    if updates:
        existing_creatives = {}
        statement = dfp.FilterStatement(
            "WHERE id IN (%s)" % 
                ", ".join([str(link.dfp_creative_id) for link in updates]))

        while True:
            response = dfp_creative_service.execute(
                "getCreativesByStatement",
                statement.ToStatement(),
            )

            if "results" in response:
                for creative in response["results"]:
                    existing_creatives[creative.id] = creative
                statement.offset += dfp.SUGGESTED_PAGE_LIMIT
            else:
                break


        updated = dfp_creative_service.execute("updateCreatives", [
            _link_to_creative(
                link=link,
                existing=existing_creatives[link.dfp_creative_id],
            )
        for link in updates])

        creatives += updated

    if inserts:
        authors = Account._byID([link.author_id for link in inserts], return_dict=False)
        advertisers = advertisers_service.bulk_upsert(authors)
        advertisers_by_author = {
            advertiser.externalId: advertiser
        for advertiser in advertisers}

        inserted = dfp_creative_service.execute("createCreatives", [
            _link_to_creative(
                link=link,
                advertiser=advertisers_by_author[
                    Account._fullname_from_id36(to36(link.author_id))
                ],
            )
        for link in inserts])

        creatives += inserted

    creatives_by_fullname = {
        utils.get_template_variable(creative, "link_id"): creative
    for creative in creatives}

    for link in links:
        creative = creatives_by_fullname[link._fullname]
        link.dfp_creative_id = creative.id
        link._commit()

    return creatives

