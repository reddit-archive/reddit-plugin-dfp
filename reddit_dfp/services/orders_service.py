from googleads import dfp
from pylons import g

from reddit_dfp.services import (
    advertisers_service,
    authentication_service,
)

dfp_client = authentication_service.get_client()
dfp_order_service = dfp_client.GetService("OrderService", version=g.dfp_service_version)

def get_order(user):
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
        "key": "traffickerId",
        "value": {
            "xsi_type": "NumberValue",
            "value": g.dfp_selfserve_trafficker_id,
        },
    }]

    query = "WHERE advertiserId = :advertiserId AND traffickerId = :traffickerId"
    statement = dfp.FilterStatement(query, values, 1)

    response = dfp_order_service.getOrdersByStatement(
                statement.ToStatement())

    if ("results" in response and len(response["results"])):
        return response["results"][0]
    else:
        return None


def create_order(user):
    advertiser_id = getattr(user, "dfp_advertiser_id", None)

    if not advertiser_id:
        raise ValueError("user doesn't have a dfp advertiser account: %s" % user._id)

    orders = [{
        "name": "%s-selfserve" % user.name,
        "advertiserId": advertiser_id,
        "salespersonId": g.dfp_selfserve_salesperson_id,
        "traffickerId": g.dfp_selfserve_trafficker_id,
        "externalOrderId": user._fullname,
    }]

    orders = dfp_order_service.createOrders(orders)

    return orders[0]


def upsert_order(user):
    order = get_order(user)

    if order:
        return order

    return create_order(user)

