import sys
from oslo_config import cfg
from logging import getLogger

CONF = cfg.CONF


class Server(object):
    cmd_name = 'server'

    @classmethod
    def add_argument_parser(cls, subparsers):
        parser = subparsers.add_parser(cls.cmd_name, help='This command runs server')
        parser.set_defaults(cmd_class=cls)

    def run(self):
        print("Server is running")


class Client(object):
    cmd_name = 'client'

    @classmethod
    def add_argument_parser(cls, subparsers):
        parser = subparsers.add_parser(cls.cmd_name, help='This command runs client')
        parser.set_defaults(cmd_class=cls)

    def run(self):
        print("Client is running")

APPS = [Server, Client]


def add_command_parsers(subparsers):
    for app in APPS:
        app.add_argument_parser(subparsers)


OPTION_LIST = [COMMAND] = [
    cfg.SubCommandOpt(name='command', title='Commands',
                      handler=add_command_parsers,
                      help='Available commands')
]

LOG = getLogger(__name__)

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

