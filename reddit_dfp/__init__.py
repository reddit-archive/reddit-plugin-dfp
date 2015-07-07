from pylons import g

from r2.lib.configparse import ConfigValue
from r2.lib.js import Module
from r2.lib.plugin import Plugin


class Dfp(Plugin):
    needs_static_build = False

    config = {
        ConfigValue.int: [
            "dfp_network_code",
            "dfp_test_network_code",
            "dfp_selfserve_salesperson_id",
            "dfp_selfserve_trafficker_id",
        ],
        ConfigValue.str: [
            "dfp_project_id",
            "dfp_client_id",
            "dfp_service_account_email",
            "dfp_cert_fingerprint",
            "dfp_service_version",
            "dfp_selfserve_template_name",
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

    def load_selfserve_template(self):
        from reddit_dfp.services import template_service

        name = g.dfp_selfserve_template_name
        template = template_service.get_template_by_name(
            name)

        if not template:
            raise ValueError("cannot find template '%s'" % name)

        g.dfp_selfserve_template_id = int(template.id)

    def load_controllers(self):
        from reddit_dfp.controllers.linkcontroller import LinkController
        from reddit_dfp.hooks import hooks
        from reddit_dfp.lib import dfp

        dfp.load_client()
        hooks.register_all()

        self.load_selfserve_template()
