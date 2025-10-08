import os
import sys
import asyncclick as click


class Config:
    def __init__(self):
        self.home = os.getcwd()

    def log(self, msg, *args):
        """Logs message to strerr."""
        if args:
            msg = msg % args
        click.echo(msg, file=sys.stderr)


config = click.make_pass_decorator(Config, ensure=True)
