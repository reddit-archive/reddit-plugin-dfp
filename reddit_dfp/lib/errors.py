from pylons import g

from reddit_dfp.lib.object import deep_getattr

DEFAULT_REASON = "UNKNOWN"

def get_reason(webfault):
    g.log.debug("dfp soap exception: %s" % webfault)

    errors = deep_getattr(webfault, "fault.detail.ApiExceptionFault.errors")

    if not errors:
        return DEFAULT_REASON

    return (errors.reason if hasattr(errors, "reason") else
        errors.errorString if hasattr(errors, "errorString") else
            DEFAULT_REASON) 


class RateLimitException(Exception):
    pass

