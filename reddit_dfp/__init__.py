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
            "dfp_selfserve_mobile_web_placement_name",
            "dfp_selfserve_dekstop_placement_name",
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

    def load_cached_ids(self):
        from reddit_dfp.services import (
            placement_service,
            template_service,
        )

        template = template_service.get_template_by_name(
            g.dfp_selfserve_template_name)

        if not template:
            raise ValueError("cannot find template '%s'" % g.dfp_selfserve_template_name)

        mobile_web_placement = placement_service.get_placement_by_name(
            g.dfp_selfserve_mobile_web_placement_name)
        desktop_placement = placement_service.get_placement_by_name(
            g.dfp_selfserve_dekstop_placement_name)

        if not mobile_web_placement:
            raise ValueError("cannot find placement '%s'"
                % g.dfp_selfserve_mobile_web_placement_name)

        if not desktop_placement:
            raise ValueError("cannot find placement '%s'"
                % g.dfp_selfserve_dekstop_placement_name)

        g.dfp_selfserve_template_id = int(template.id)
        g.dfp_selfserve_mobile_web_placement_id = int(mobile_web_placement.id)
        g.dfp_selfserve_desktop_placement_id = int(desktop_placement.id)

    def load_controllers(self):
        from reddit_dfp.controllers.linkcontroller import LinkController
        from reddit_dfp.hooks import hooks
        from reddit_dfp.lib import dfp

        dfp.load_client()
        hooks.register_all()

        self.load_cached_ids()
