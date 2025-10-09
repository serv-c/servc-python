
import asyncclick as click

from servc.cli.config import config
from servc.cli.commands import init


@click.group()
@config
async def cli(ctx):
    """servc CLI - A tool for servc services.

    Create and manage servc projects.

    Common commands:
    
      init    Initialize a new servc project
    """
    pass

cli.add_command(init)
