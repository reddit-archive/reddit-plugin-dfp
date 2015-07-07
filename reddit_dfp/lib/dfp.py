from googleads import dfp
from googleads import oauth2
from os import path
from pylons import g
from suds import WebFault

from reddit_dfp.lib import errors

KEY_FILE = path.join(path.dirname(path.abspath(__file__)), "../../id_dfp")

_client = None

def load_client():
    global _client

    if not _client:
        oauth2_client = oauth2.GoogleServiceAccountClient(
            oauth2.GetAPIScope("dfp"),
            g.dfp_service_account_email,
            KEY_FILE,
        )

        _client = dfp.DfpClient(oauth2_client, g.dfp_project_id)
        _client.network_code = g.dfp_test_network_code if g.debug else g.dfp_network_code


def get_service(service):
    return _client.GetService(service, version=g.dfp_service_version)


def get_downloader():
    return _client.GetDataDownloader(version=g.dfp_service_version)


class DfpService():
    def __init__(self, service_name, retries=3, delay_exponent=2):
        self.service = get_service(service_name)
        self.retries = 3
        self.delay_exponent = delay_exponent
        self.attempt = 1

    def execute(self, method, *args, **kwargs):
        response = None
        call = getattr(self.service, method)
        while response == None and self.attempt <= self.retries:
            try:
                response = call(*args, **kwargs)
            except WebFault as e:
                if errors.get_reason(e) == "EXCEEDED_QUOTA":
                    wait = self.attempt ** self.delay_exponent
                    g.log.debug("failed attempt %d, retrying in %d seconds." % (self.attempt, wait))
                    time.sleep(wait)
                    self.attempt += 1
                else:
                    raise e

        if not response and self.attempt == self.retries:
            raise errors.RateLimitException("failed after %d attempts" % self.attempt)

        return response
