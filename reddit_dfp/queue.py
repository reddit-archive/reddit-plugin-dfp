import json

from collections import defaultdict
from datetime import datetime, timedelta
from pylons import g

from r2.config import feature
from r2.lib import (
    amqp,
)

from reddit_dfp.lib.errors import RateLimitException

DFP_QUEUE = "dfp_q"
RATE_LIMIT_ENDS_AT = "dfp-rate-limit-ends-at"


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
    from r2.models import (
        Account,
        Link,
        NotFound,
        PromoCampaign,
    )

    from reddit_dfp.services import (
        creatives_service,
        lineitems_service,
    )

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


    def _handle_check_edits(payload):
        creative = creatives_service.get_creative(link)

        link = utils.dfp_creative_to_link(
            creative, link=Link._by_fullname(payload["link"], data=True))

        link.dfp_checking_edits = False
        link._commit()


    processor = Processor()

    if feature.is_enabled("dfp_selfserve"):
        processor.register("upsert_promotion", _handle_upsert_promotion)
        processor.register("upsert_campaign", _handle_upsert_campaign)
        processor.register("deactivate_campaign", _handle_deactivate_campaign)

    processor.register("check_edits", _handle_check_edits)

    @g.stats.amqp_processor(DFP_QUEUE)
    def _handler(message):
        rate_limit_ends_at = g.cache.get(RATE_LIMIT_ENDS_AT)
        now_utc = datetime.utcnow()

        if rate_limit_ends_at:
            if now_utc > rate_limit_ends_at:
                g.cache.delete(RATE_LIMIT_ENDS_AT)
            else:
                raise RateLimitException("waiting until %s" % rate_limit_ends_at)

        data = json.loads(message.body)
        g.log.debug("processing action: %s" % data)

        action = data.get("action")
        payload = data.get("payload")

        try:
            processor.call(action, payload)
        except RateLimitException as e:
            g.cache.set(RATE_LIMIT_ENDS_AT, datetime.utcnow() + timedelta(minutes=1))
            raise e

    amqp.consume_items(DFP_QUEUE, _handler, verbose=False)


def push(action, payload):
    g.log.debug("%s: queuing action \"%s\"" % (DFP_QUEUE, action))
    message = json.dumps({
        "action": action,
        "payload": payload,
    })
    amqp.add_item(DFP_QUEUE, message)

