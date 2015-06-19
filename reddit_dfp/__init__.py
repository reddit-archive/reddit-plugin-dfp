from r2.lib.configparse import ConfigValue
from r2.lib.js import Module
from r2.lib.plugin import Plugin


class Dfp(Plugin):
    needs_static_build = False

    config = {
        ConfigValue.int: [
            "dfp_network_code",
            "dfp_selfserve_salesperson_id",
            "dfp_selfserve_trafficker_id",
            "dfp_selfserve_template_id",
        ],
        ConfigValue.str: [
            "dfp_project_id",
            "dfp_client_id",
            "dfp_service_account_email",
            "dfp_cert_fingerprint",
            "dfp_service_version",
        ],
    }

    def declare_queues(self, queues):
        from r2.config.queues import MessageQueue
        from reddit_dfp import queue

        queues.declare({
            queue.DFP_QUEUE: MessageQueue(bind_to_self=True),
        })

    def add_routes(self, mc):
        mc("/api/dfp/link", controller="link", action="link_from_id")

    def load_controllers(self):
        from reddit_dfp.controllers.linkcontroller import LinkController
        from reddit_dfp.hooks import hooks
        from reddit_dfp.lib import dfp

        dfp.load_client()
        hooks.register_all()
