import sys
from oslo_config import cfg
import oslo_messaging as messaging
import logging
import json
import time

CONF = cfg.CONF


class NotificationHandler(object):
    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        self.handle_message(ctxt, publisher_id, event_type, payload, metadata, level="INFO")

    def warn(self, ctxt, publisher_id, event_type, payload, metadata):
        self.handle_message(ctxt, publisher_id, event_type, payload, metadata, level="WARN")

    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        self.handle_message(ctxt, publisher_id, event_type, payload, metadata, level="ERROR")

    def handle_message(self, ctx, publisher_id, event_type, payload, metadata, level):
        LOG.info("Message with %s level received." % level)
        print ("publisher id: \t %s" % publisher_id)
        print ("event type: \t %s" % event_type)
        print ("payload: ")
        print json.dumps(dict(payload), separators=(',', ':'), indent=4)


class ServerApp(object):
    cmd_name = 'server'

    def __init__(self):
        self.transport = messaging.get_transport(cfg.CONF)
        self.targets = [messaging.Target(topic='notification')]
        self.endpoints = [NotificationHandler()]
        self.server = messaging.get_notification_listener(self.transport, self.targets,
                                                          self.endpoints, executor='blocking',
                                                          pool='test')

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
        LOG.info("Server is terminated")


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
        parser.add_argument('-i', '--info',  action='store_true', default=False,
                            help='INFO Message level')

        parser.add_argument('-w', '--warn',  action='store_true', default=False,
                            help='WARN Message level')

        parser.add_argument('-e', '--error',  action='store_true', default=False,
                            help='ERROR Message level')

        parser.add_argument('-p', '--producer-id', default="producer",
                            help="Sets producer_id")

        parser.add_argument('json', metavar='<path to json file>', help="Path to JSON file")

    def run(self):
        try:
            with open(CONF.command.json) as json_data:
                data = json.load(json_data)
                json_data.close()
        except IOError as e:
            LOG.error(e)
            sys.exit(1)
        except ValueError as e:
            LOG.error("%s file validation error. %s" % (CONF.command.json, e))
            sys.exit(1)

        if CONF.command.info:
            self.notifier.info({}, 'just.testing', data)
        if CONF.command.warn:
            self.notifier.warn({}, 'just.testing', data)
        if CONF.command.error:
            self.notifier.error({}, 'just.testing', data)


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

