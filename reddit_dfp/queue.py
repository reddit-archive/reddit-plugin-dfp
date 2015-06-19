import json

from collections import defaultdict
from pylons import g

from r2.lib import (
    amqp,
)
from r2.models import (
    Account,
    Link,
    NotFound,
    PromoCampaign,
)

from reddit_dfp.services import lineitems_service


DFP_QUEUE = "dfp_q"


class Processor():
    def __init__(self):
        self._handlers = defaultdict(list)

    def get_handlers(action):
        return self._handlers[action]

    def call(self, action, *args, **kwargs):
        handlers = self.get_handlers(action)
        results = []

        for handler in handlers:
            results.append(handler(*args, **kwargs))

        return results

    def register(self, action, handler):
        existing = self.get_handlers(action)
        action.append(handler)


def process():
    processor = Processor()
    processor.register("upsert_promotion", _handle_upsert_promotion)
    processor.register("upsert_campaign", _handle_upsert_campaign)
    processor.register("deactivate_campaign", _handle_deactivate_campaign)

    @g.stats.amqp_processor(DFP_QUEUE)
    def _handler(message):
        data = json.loads(message.body)
        g.log.debug("processing action: %s" % data)

        action = data.get("action")
        payload = data.get("payload")

        processor.call(action, payload)

    amqp.consume_items(DFP_QUEUE, _handler, verbose=False)


def push(action, payload):
    g.log.debug("%s: queuing action \"%s\"" % (DFP_QUEUE, action))
    message = json.dumps({
        "action": action,
        "payload": payload,
    })
    amqp.add_item(DFP_QUEUE, message)


def _handle_upsert_promotion(payload):
    link = Link._by_fullname(payload["link"], data=True)
    author = Account._byID(link.author_id)

    creatives_service.upsert_creative(author, link)


def _handle_upsert_campaign(payload):
    link = Link._by_fullname(payload["link"], data=True)
    campaign = PromoCampaign._by_fullname(payload["campaign"], data=True)
    owner = Account._byID(campaign.owner_id)

    lineitem = lineitems_service.upsert_lineitem(owner, campaign)
    creative = creatives_service.get_creative(link)

    lineitems_service.associate_with_creative(
        lineitem=lineitem, creative=creative)


def _handle_deactivate_campaign(payload):
    campaign = PromoCampaign._by_fullname(payload["campaign"])

    lineitem = lineitems_service.get_lineitem(campaign)
    if lineitem:
        lineitems_service.deactivate(lineitem)

