from datetime import datetime, timedelta
from pylons import g

from r2.controllers import add_controller
from r2.controllers.oauth2 import allow_oauth2_access
from r2.controllers.reddit_base import (
    cross_domain,
    RedditController,
)
from r2.lib.base import abort
from r2.lib.errors import errors
from r2.lib.pages.things import (
    wrap_links,
)
from r2.lib.validator import (
    json_validate,
    VInt,
    VModhashIfLoggedIn,
)
from r2.models import (
    Account,
    Link,
    Subreddit,
)
from r2.models.trylater import TryLater


from reddit_dfp import queue
from reddit_dfp.lib import utils
from reddit_dfp.models.cache import LinksByDfpCreativeId
from reddit_dfp.services import creatives_service


def _check_edits(link):
    checking_edits = getattr(link, "dfp_checking_edits", False)

    if not checking_edits:
        TryLater.schedule("dfp_check_edits", link._fullname, timedelta(minutes=5))

        link.dfp_checking_edits = True
        link._commit()


@add_controller
class LinkController(RedditController):
    @cross_domain(allow_credentials=True)
    @allow_oauth2_access
    @json_validate(
        VModhashIfLoggedIn(),
        dfp_creative_id=VInt("dfp_creative_id", min=0),
    )
    def POST_link_from_id(self, responder, dfp_creative_id, *a, **kw):
        if (responder.has_errors("dfp_creative_id", errors.BAD_NUMBER)):
            return

        link = LinksByDfpCreativeId.get(dfp_creative_id)

        if link:
            _check_edits(link)

        if not link:
            try:
                creative = creatives_service.by_id(dfp_creative_id)
            except Exception as e:
                g.log.error("dfp error: %s" % e)
                abort(404)

            link = utils.dfp_creative_to_link(creative)

            LinksByDfpCreativeId.add(link)

        listing = wrap_links([link])
        thing = listing.things[0]

        return thing.render()

