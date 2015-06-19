from googleads import dfp
from pylons import g

from r2.models import promo

from reddit_dfp.lib.merge import merge_deep
from reddit_dfp.services import (
    authentication_service,
    orders_service,
)

ONE_MICRO_DOLLAR = 1000000
NATIVE_SIZE = {
    "width": "1",
    "height": "1",
}
LINE_ITEM_DEFAULTS = {
    "creativeRotationType": "OPTIMIZED",
    "creativePlaceholders": [{
        "size": NATIVE_SIZE
    }],
    "reserveAtCreation": False,
    "primaryGoal": {
        "goalType": "DAILY",
        "unitType": "IMPRESSIONS",
        "units": 0,
    },
    "targeting": {
        "inventoryTargeting": {
            "targetedAdUnits": [{
                "adUnitId": "mw_card_test_1",
            }],
        },
    },
}

dfp_client = authentication_service.get_client()
dfp_lineitems_service = dfp_client.GetService(
    "LineItemService", version=g.dfp_service_version)
dfp_lica_service = dfp_client.GetService(
    "LineItemCreativeAssociationService", version=g.dfp_service_version)


def _date_to_string(date, format="%d/%m/%y"):
    return date.strftime(format)


def _get_campaign_name(campaign):
    return ("%s [%s-%s]" % (
                    campaign.link_id,
                    _date_to_string(campaign.start_date),
                    _date_to_string(campaign.end_date)))[:255]

def _get_platform(campaign):
    if campaign.platform == "desktop":
        return "WEB"
    if campaign.platform == "mobile":
        return "MOBILE"
    else:
        return "ANY"


def _get_cost_type(campaign):
    return "CPM" # everything is CPM currently


def _date_to_dfp_datetime(date, hour, timezone_id=g.dfp_timezone_id):
    return {
        "date": {
            "year": date.year,
            "month": date.month,
            "day": date.day,
        },
        "hour": str(hour),
        "minute": "0",
        "second": "0",
        "timeZoneID": timezone_id,
    }


def _priority_to_lineitem_type(priority):
    if priority == promo.HIGH:
        return "SPONSORSHIP"
    elif priority == promo.MEDIUM:
        return "STANDARD"
    elif priority == promo.REMNANT:
        return "BULK"
    elif priority == promo.HOUSE:
        return "HOUSE"


def _dollars_to_money(dollars):
    return {
        "currencyCode": "USD",
        "microAmount": dollars * ONE_MICRO_DOLLAR,
    }


def _campaign_to_lineitem(campaign, order=None, existing=None):
    if not (existing or order):
        raise ValueError("must either pass an order or an existing lineitem.")

    lineitem = {
        "name": _get_campaign_name(campaign),
        "startDateTime": _date_to_dfp_datetime(campaign.start_date),
        "endDateTime": _date_to_dfp_datetime(campaign.end_date),
        "lineItemType": _priority_to_lineitem_type(campaign.priority),
        "costPerUnit": _dollars_to_money(campaign.cpm / 100),
        "costType": _get_cost_type(campaign),
        "targetPlatform": _get_platform(campaign),
        "skipInventoryCheck": campaign.priority.inventory_override,
        "primaryGoal": {
            "units": campaign.impressions,
        },
    }

    if existing:
        return merge_deep(existing, lineitem)
    else:
        return merge_deep(lineitem, LINE_ITEM_DEFAULTS, {
            "orderId": order["id"],
            "externalId": campaign._fullname,
        })


def get_lineitem(campaign):
    values = [{
        "key": "externalId",
        "value": {
            "xsi_type": "TextValue",
            "value": campaign._fullname,
        },
    }]
    query = "WHERE externalId = :externalId"
    statement = dfp.FilterStatement(query, values, 1)
    response = dfp_lineitems_service.getLineItemsByStatement(
                    statement.ToStatement())

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None

def create_lineitem(user, campaign):
    order = orders_service.upsert_order(user)

    lineitem = _campaign_to_lineitem(campaign, order=order)
    lineitems = dfp_lineitems_service.createLineItems([lineitem])

    return lineitems[0]

def upsert_lineitem(user, campaign):
    lineitem = get_lineitem(campaign)

    if not lineitem:
        return create_lineitem(user, campaign)

    if lineitem["isArchived"]:
        raise ValueError("cannot update archived lineitem (lid: %s, cid: %s)" %
                (lineitem["id"], campaign._id))

    updated = _campaign_to_lineitem(campaign, existing=lineitem)
    lineitems = dfp_lineitems_service.updateLineItems([updated])

    return lineitems[0]


def associate_with_creative(lineitem, creative):
    lineitem_id = lineitem["id"]
    creative_id = creative["id"]

    values = [{
        "key": "lineItemId",
        "value": {
            "xsi_type": "NumberValue",
            "value": lineitem_id,
        },
    }, {
        "key": "creativeId",
        "value": {
            "xsi_type": "NumberValue",
            "value": creative_id,
        },
    }]
    query = "WHERE lineItemId = :lineItemId AND creativeId = :creativeId"
    statement = dfp.FilterStatement(query, values, 1)

    response = dfp_lica_service.getLineItemCreativeAssociationsByStatement(
                    statement.ToStatement())

    if ("results" in response and len(response["results"])):
        association = response["results"][0]
    else:
        associations = dfp_lica_service.createLineItemCreativeAssociations([{
            "lineItemId": lineitem_id,
            "creativeId": creative_id,
        }])

        return associations[0]


def deactivate(campaign):
    lineitem = get_lineitem(campaign)

    if not lineitem:
        return True

    lineitem_id = lineitem["id"]
    values = [{
        "key": "lineItemId",
        "value": {
            "xsi_type": "NumberValue",
            "value": lineitem_id
        },
    }, {
        "key": "status",
        "value": {
            "xsi_type": "TextValue",
            "value": "ACTIVE"
        },
    }]

    query = "WHERE lineItemId = :lineItemId AND status = :status"
    statement = dfp.FilterStatement(query, values, 1)

    response = dfp_lica_service.getLineItemCreativeAssociationsByStatement(
            statement.ToStatement())

    return result and int(result["numChanges"]) > 0

