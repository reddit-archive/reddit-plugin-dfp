from datetime import datetime
from googleads import dfp
from pylons import g

from reddit_dfp.lib import utils
from reddit_dfp.lib.dfp import DfpService
from reddit_dfp.lib.merge import merge_deep
from reddit_dfp.services import (
    orders_service,
)

NATIVE_SIZE = {
    "width": "1",
    "height": "1",
}
LINE_ITEM_DEFAULTS = {
    "creativeRotationType": "OPTIMIZED",
    "creativePlaceholders": [{
        "size": NATIVE_SIZE,
        "creativeSizeType": "INTERSTITIAL",
    }],
    "reserveAtCreation": False,
    "targeting": {
        "inventoryTargeting": {
            "targetedPlacementIds": [1712944],
        },
    },
}


def _date_to_string(date, format="%d/%m/%y"):
    return date.strftime(format)


def _get_campaign_name(campaign):
    return ("%s [%s-%s]" % (
                    campaign.link_id,
                    _date_to_string(campaign.start_date),
                    _date_to_string(campaign.end_date)))[:255]


def _get_cost_type(campaign):
    return "CPM" # everything is CPM currently


def _get_goal_type(campaign):
    if campaign.impressions > 0:
        return {
            "goalType": "LIFETIME",
            "unitType": "IMPRESSIONS",
            "units": campaign.impressions,
        }

    return {
        "goalType": "NONE",
    }


def _priority_to_lineitem_type(priority):
    from r2.models import promo

    if priority == promo.HIGH:
        return "SPONSORSHIP"
    elif priority == promo.MEDIUM:
        return "STANDARD"
    elif (priority == promo.REMNANT or
          priority == promo.HOUSE):
        return "PRICE_PRIORITY"


def _campaign_to_lineitem(campaign, order=None, existing=None):
    if not (existing or order):
        raise ValueError("must either pass an order or an existing lineitem.")

    lineitem = {
        "name": _get_campaign_name(campaign),
        "lineItemType": _priority_to_lineitem_type(campaign.priority),
        "costPerUnit": utils.pennies_to_dfp_money(campaign.cpm),
        "costType": _get_cost_type(campaign),
        "targetPlatform": "ANY", # other targets are deprecated
        "skipInventoryCheck": campaign.priority.inventory_override,
        "primaryGoal": _get_goal_type(campaign),
    }

    if existing is None:
        # TODO: non-global timezone_id
        now = datetime.today()
        now = now.replace(tzinfo=campaign.start_date.tzinfo)
        start_date = campaign.start_date
        end_date = campaign.end_date

        lineitem["startDateTime"] = utils.datetime_to_dfp_datetime(
            start_date, timezone_id=g.dfp_timezone_id)
        lineitem["endDateTime"] = utils.datetime_to_dfp_datetime(
            end_date, timezone_id=g.dfp_timezone_id)

        if start_date < now:
            lineitem["startDateTimeType"] = "IMMEDIATELY"

        if end_date < now:
            raise ValueError("can't creative lineitem that ends in the past. (%s-%s)" % (start_date, end_date))

    if existing:
        return merge_deep(existing, lineitem)
    else:
        return merge_deep(lineitem, LINE_ITEM_DEFAULTS, {
            "orderId": order["id"],
            "externalId": campaign._fullname,
        })


def get_lineitem(campaign):
    dfp_lineitem_service = DfpService("LineItemService")

    values = [{
        "key": "externalId",
        "value": {
            "xsi_type": "TextValue",
            "value": campaign._fullname,
        },
    }]
    query = "WHERE externalId = :externalId"
    statement = dfp.FilterStatement(query, values, 1)
    response = dfp_lineitem_service.execute(
                    "getLineItemsByStatement",
                    statement.ToStatement())

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


def create_lineitem(user, campaign):
    dfp_lineitem_service = DfpService("LineItemService")
    order = orders_service.upsert_order(user)

    try:
        lineitem = _campaign_to_lineitem(campaign, order=order)
    except ValueError as e:
        g.log.debug("unable to convert campaign to valid line item for campaign %s" % campaign._fullname)
        raise e
    lineitems = dfp_lineitem_service.execute("createLineItems", [lineitem])

    return lineitems[0]


def upsert_lineitem(user, campaign):
    dfp_lineitem_service = DfpService("LineItemService")
    lineitem = get_lineitem(campaign)

    if not lineitem:
        return create_lineitem(user, campaign)

    if lineitem["isArchived"]:
        raise ValueError("cannot update archived lineitem (lid: %s, cid: %s)" %
                (lineitem["id"], campaign._id))

    updated = _campaign_to_lineitem(campaign, existing=lineitem)
    lineitems = dfp_lineitem_service.execute("updateLineItems", [updated])

    return lineitems[0]


def associate_with_creative(lineitem, creative):
    dfp_association_service = DfpService("LineItemCreativeAssociationService")

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

    response = dfp_association_service.execute(
                    "getLineItemCreativeAssociationsByStatement",
                    statement.ToStatement())

    if ("results" in response and len(response["results"])):
        association = response["results"][0]
    else:
        associations = dfp_association_service.execute(
            "createLineItemCreativeAssociations",
            [{
                "lineItemId": lineitem_id,
                "creativeId": creative_id,
            }])

        return associations[0]


def _perform_lineitem_action(action, query):
    dfp_lineitem_service = DfpService("LineItemService")
    statement = dfp.FilterStatement(query)
    response = dfp_lineitem_service.execute(
        "performLineItemAction",
        {"xsi_type": action},
        statement.ToStatement())

    return response


def activate(campaigns):
    if not isinstance(campaigns, list):
        campaigns = [campaigns]

    query = "WHERE status IN (%(status)s) and externalId IN (%(ids)s)"
    ids = ",".join(["'" + campaign + "'" for campaign in campaigns])

    try:
        _perform_lineitem_action(
            action="ActivateLineItems",
            query=(query % {"status": "'DRAFT', 'INACTIVE'", "ids": ids}))
        _perform_lineitem_action(
            action="ResumeLineItems",
            query=(query % {"status": "'PAUSE'", "ids": ids}))
    except:
        return False

    return True


def deactivate(campaigns):
    if not isinstance(campaigns, list):
        campaigns = [campaigns]

    query = ("WHERE NOT status IN ('DRAFT', 'INACTIVE', 'PAUSED') " +
             " and externalId IN (%s)" % 
             ",".join(["'" + campaign + "'" for campaign in campaigns]))

    try:
        _perform_lineitem_action(action="PauseLineItems", query=query)
    except:
        return False

    return True

