from googleads import dfp
from pylons import g

from reddit_dfp.lib.dfp import DfpService
from reddit_dfp.services import (
    advertisers_service,
)


def get_order(user):
    dfp_order_service = DfpService("OrderService")
    advertiser = advertisers_service.upsert_advertiser(user)
    advertiser_id = advertiser.id

    if not advertiser_id:
        return None

    values = [{
        "key": "advertiserId",
        "value": {
            "xsi_type": "NumberValue",
            "value": advertiser_id,
        },
    }, {
        "key": "traffickerId",
        "value": {
            "xsi_type": "NumberValue",
            "value": g.dfp_selfserve_trafficker_id,
        },
    }]

    query = "WHERE advertiserId = :advertiserId AND traffickerId = :traffickerId"
    statement = dfp.FilterStatement(query, values, 1)

    response = dfp_order_service.execute(
                "getOrdersByStatement",
                statement.ToStatement())

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


def create_order(user):
    dfp_order_service = DfpService("OrderService")
    advertiser_id = getattr(user, "dfp_advertiser_id", None)

    if not advertiser_id:
        raise ValueError("user doesn't have a dfp advertiser account: %s" % user._id)

    orders = [{
        "name": "%s-selfserve" % user.name,
        "advertiserId": advertiser_id,
        "salespersonId": g.dfp_selfserve_salesperson_id,
        "traffickerId": g.dfp_selfserve_trafficker_id,
        "externalOrderId": user._id,
    }]

    orders = dfp_order_service.execute("createOrders", orders)

    return orders[0]


def upsert_order(user):
    order = get_order(user)

    if order:
        return order

    return create_order(user)

