from googleads import dfp
from googleads import oauth2
from os import path
from pylons import g


KEY_FILE = path.join(path.dirname(path.abspath(__file__)), "../../id_dfp")

oauth2_client = oauth2.GoogleServiceAccountClient(
    oauth2.GetAPIScope("dfp"),
    g.dfp_service_account_email,
    KEY_FILE,
)

_client = dfp.DfpClient(oauth2_client, g.dfp_project_id)
_client.network_code = g.dfp_network_code

print "code", _client.network_code

def get_client():
    return _client
