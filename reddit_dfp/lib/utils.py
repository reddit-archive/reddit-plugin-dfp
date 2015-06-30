# coding=utf-8

from datetime import datetime
from pylons import g

ONE_MICRO_DOLLAR = 1000000


def get_dfp_subreddit():
    from r2.models import Subreddit
    return Subreddit._byID(Subreddit.get_promote_srid())


def get_dfp_user():
    from r2.models import Account
    return Account._by_name(g.dfp_user)


def dfp_datetime_to_datetime(dfp_datetime):
    year = dfp_datetime["date"]["year"]
    month = dfp_datetime["date"]["month"]
    day = dfp_datetime["date"]["day"]
    hour = dfp_datetime["hour"]
    minute = dfp_datetime["minute"]
    second = dfp_datetime["second"]

    return datetime(year, month, day, hour, minute, second)


def datetime_to_dfp_datetime(datetime, timezone_id=None):
    if timezone_id is None:
        timezone_id = g.dfp_timezone_id
    return {
        "date": {
            "year": datetime.year,
            "month": datetime.month,
            "day": datetime.day,
        },
        "hour": getattr(datetime, "hour", 0),
        "minute": getattr(datetime, "minute", 0),
        "second": getattr(datetime, "second", 0),
        "timeZoneID": timezone_id,
    }


def trim(string, length, ellipsis=u"…"):
    return string[:length-1] + ellipsis if len(string) > length else string


def dollars_to_dfp_money(dollars):
    return {
        "currencyCode": "USD",
        "microAmount": dollars * ONE_MICRO_DOLLAR,
    }


def dfp_template_to_dict(template):
    result = {}
    for definition in template:
        key = definition["uniqueName"]
        value = getattr(definition, "value", None)
        if value:
            value = str(value)

        result[key] = value

    return result


def dfp_creative_to_link(creative, link=None):
    from r2.models import (
        Link,
        PROMOTE_STATUS,
    )

    user = get_dfp_user()
    sr = get_dfp_subreddit()
    attributes = dfp_template_to_dict(
        creative.creativeTemplateVariableValues)

    kind = "self" if attributes["selftext"] else "link"
    url = attributes["url"] if kind == "link" else "self"

    if not link:
        link = Link._submit(
            attributes["title"], url, user, sr,
            ip="127.0.0.1", sendreplies=False,
        )

    if kind == "self":
        link.url = link.make_permalink_slow()
        link.is_self = True
        link.selftext = attributes["selftext"]

    link.promoted = True
    link.promote_status = PROMOTE_STATUS.promoted
    link.thumbnail_url = attributes["thumbnail_url"]
    link.mobile_ad_url = attributes["mobile_ad_url"]
    link.third_party_tracking = attributes["third_party_tracking"]
    link.third_party_tracking_2 = attributes["third_party_tracking_2"]
    link.dfp_creative_id = creative["id"]

    link._commit()
    return link
