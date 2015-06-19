# coding=utf-8

from googleads import dfp
from pylons import g

from r2.models import (
    Account,
    promo,
)

from reddit_dfp.lib.dfp import get_service
from reddit_dfp.lib.merge import merge_deep
from reddit_dfp.services import (
    lineitems_service,
    advertisers_service,
)

NATIVE_SIZE = {
    "width": "1",
    "height": "1",
}


def _trim(string, length):
    return string[:length-1] + u"…" if len(string) > length else string


def _get_creative_name(link):
    return "%s [%s]" % (_trim(link.title, 150), _trim(link.url, 100))


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
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "url",
            "value": link.url,
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "selftext",
            "value": link.selftext
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "thumbnail_url",
            "value": link.thumbnail_url,
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "mobile_ad_url",
            "value": link.mobile_ad_url,
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "third_party_tracking",
            "value": link.third_party_tracking,
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "third_party_tracking_2",
            "value": link.third_party_tracking_2,
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
            "advertiserId": advertiser_id,
        })


def get_creative(link):
    creative_id = getattr(link, "dfp_creative_id", None)

    if not creative_id:
        return None

    return by_id(creative_id)


def by_id(creative_id):
    dfp_creatives_service = get_service("CreativeService")

    values = [{
        "key": "id",
        "value": {
            "xsi_type": "NumberValue",
            "value": creative_id,
        },
    }]
    query = "WHERE id = :id"
    statement = dfp.FilterStatement(query, values, 1)

    response = dfp_creatives_service.getCreativesByStatement(
                    statement.ToStatement())

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


def create_creative(user, link):
    dfp_creatives_service = get_service("CreativeService")
    advertiser = advertisers_service.upsert_advertiser(user)
    creative = _link_to_creative(link, advertiser=advertiser)

    return dfp_creatives_service.createCreative(creative)

def upsert_creative(user, link):
    dfp_creatives_service = get_service("CreativeService")
    creative = get_creative(link)

    if not creative:
        return create_creative(user, link)

    updated = _link_to_creative(link, existing=lineitem)
    creatives = dfp_creatives_service.updateCreatives([updated])

    return creatives[0]

