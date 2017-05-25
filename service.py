import sys
from oslo_config import cfg
import oslo_messaging as messaging
import logging

import time

CONF = cfg.CONF


class NotificationHandler(object):
    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        if publisher_id == 'testing':
            print (payload)
            return messaging.NotificationResult.HANDLED

    def warn(self, ctxt, publisher_id, event_type, payload, metadata):
        pass

    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        pass


class ServerApp(object):
    cmd_name = 'server'

    def __init__(self):
        self.transport = messaging.get_transport(cfg.CONF)
        self.targets = [messaging.Target(topic='notification')]
        self.endpoints = [NotificationHandler()]
        self.server = messaging.get_notification_listener(self.transport, self.targets,
                                                          self.endpoints, allow_requeue=True,
                                                          executor='blocking', pool='test')

    @classmethod
    def add_argument_parser(cls, subparsers):
        parser = subparsers.add_parser(cls.cmd_name, help='This command runs server')
        parser.set_defaults(cmd_class=cls)

    def run(self):
        try:
            self.server.start()
            LOG.info("Server started. To stop server press <Ctrl+C>.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            LOG.info("Server termination started")

        self.server.stop()
        self.server.wait()
        LOG.info("Server stopped")


class ClientApp(object):
    cmd_name = 'client'

    def __init__(self):
        self.transport = messaging.get_transport(cfg.CONF)
        self.notifier = messaging.Notifier(self.transport, driver='messaging',
                                           publisher_id='testing', topics=['notification'])

    @classmethod
    def add_argument_parser(cls, subparsers):
        parser = subparsers.add_parser(cls.cmd_name, help='This command runs client')
        parser.set_defaults(cmd_class=cls)

    def run(self):
        self.notifier.info({'some': 'context'}, 'just.testing', {'heavy': 'payload'})

APPS = [ServerApp, ClientApp]


def add_command_parsers(subparsers):
    for app in APPS:
        app.add_argument_parser(subparsers)


OPTION_LIST = [COMMAND] = [
    cfg.SubCommandOpt(name='command', title='Commands',
                      handler=add_command_parsers,
                      help='Available commands')
]

LOG = logging.getLogger("Service")
LOG.setLevel(logging.INFO)
ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
ch.setFormatter(formatter)
LOG.addHandler(ch)

if __name__ == "__main__":
    CONF.register_cli_opts(OPTION_LIST)
    try:
        CONF(sys.argv[1:])
    except cfg.RequiredOptError as e:
        LOG.error(e)
        CONF.print_usage()
        sys.exit(1)
    except cfg.Error as e:
        LOG.exception(e)
        sys.exit(1)

    app = CONF.command.cmd_class()
    app.run()

