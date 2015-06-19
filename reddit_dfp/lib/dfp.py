from googleads import dfp
from googleads import oauth2
from os import path
from pylons import g


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
        _client.network_code = g.dfp_network_code


def get_service(service):
    return _client.GetService(service, version=g.dfp_service_version)


def get_downloader():
    return _client.GetDataDownloader(version=g.dfp_service_version)