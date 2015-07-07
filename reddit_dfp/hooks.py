from r2.lib import promote
from r2.lib.hooks import HookRegistrar
from r2.models import (
    PromoCampaign,
)

from reddit_dfp import queue
from reddit_dfp.services import lineitems_service
from reddit_dfp.services import creatives_service

hooks = HookRegistrar()

@hooks.on("promote.new_promotion")
@hooks.on("promote.edit_promotion")
def upsert_promotion(link):
    queue.push("upsert_promotion", {
        "link": link._fullname,
    })

    action = ("activate" 
                if promote.is_accepted(link) and not link._deleted else
                    "deactivate")
    campaigns = PromoCampaign._by_link(link)
    if not campaigns:
        return

    queue.push(action, { "campaigns": ",".join([campaign._fullname for campaign in campaigns]) })


@hooks.on("promote.new_campaign")
@hooks.on("promote.edit_campaign")
def upsert_campaign(link, campaign):
    queue.push("upsert_campaign", {
        "link": link._fullname,
        "campaign": campaign._fullname,
    })

    action = ("activate" 
                if promote.is_accepted(link) and not link._deleted else
                    "deactivate")

    queue.push(action, { "campaigns": campaign._fullname })


@hooks.on("promote.delete_campaign")
def delete_campaign(link, campaign):
    queue.push("deactivate_campaign", {
        "link": link._fullname,
        "campaign": campaign._fullname,
    })


@hooks.on("trylater.check_edits")
def check_edits(data):
    for fullname in data.values():
        queue.push("check_edits", {
            "link": fullname,
        })

