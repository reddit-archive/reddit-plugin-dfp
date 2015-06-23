from pylons import g

from r2.controllers import add_controller
from r2.controllers.oauth2 import allow_oauth2_access
from r2.controllers.reddit_base import (
    cross_domain,
    RedditController,
)
from r2.lib.base import abort
from r2.lib.errors import errors
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
from r2.lib.pages.things import (
    wrap_links,
)

from reddit_dfp.models.cache import LinksByExternalId
from reddit_dfp.services import creatives_service

def _get_subreddit():
    return Subreddit._byID(Subreddit.get_promote_srid())


def _get_user():
    return Account.system_user()


def _template_to_dict(creative):
    result = {}
    for definition in creative.creativeTemplateVariableValues:
        key = definition["uniqueName"]
        value = getattr(definition, "value", None)
        if value:
            value = str(value)

        result[key] = value

    return result


def _create_link(creative):

    """
    Creates a link to allow third party voting/commenting
    """

    user = _get_user()
    sr = _get_subreddit()
    attributes = _template_to_dict(creative)

    kind = "self" if attributes["selftext"] else "link"
    url = attributes["url"] if kind == "link" else "self"
    link = Link._submit(
        attributes["title"], url, user, sr,
        ip="127.0.0.1", sendreplies=False,
    )

    if kind == "self":
        link.url = link.make_permalink_slow()
        link.is_self = True
        link.selftext = attributes["selftext"]

    link.promoted = True
    link.third_party_promo = True
    link.thumbnail_url = attributes["thumbnail_url"]
    link.mobile_ad_url = attributes["mobile_ad_url"]
    link.third_party_tracking = attributes["third_party_tracking"]
    link.third_party_tracking_2 = attributes["third_party_tracking_2"]
    link.external_id = creative["id"]

    link._commit()
    return link


@add_controller
class LinkController(RedditController):
    @cross_domain(allow_credentials=True)
    @allow_oauth2_access
    @json_validate(
        VModhashIfLoggedIn(),
        external_id=VInt("external_id", min=0),
    )
    def POST_link_from_id(self, responder, external_id, *a, **kw):
        if (responder.has_errors("external_id", errors.BAD_NUMBER)):
            return

        link = LinksByExternalId.get(external_id)

        if not link:
            try:
                creative = creatives_service.by_id(external_id)
            except:
                abort(404)

            link = _create_link(creative)

            LinksByExternalId.add(link)

        listing = wrap_links([link])
        thing = listing.things[0]

        return thing.render()

