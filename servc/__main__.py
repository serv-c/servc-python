#!/usr/bin/env python3
"""
Main entry point for the servc CLI when run as a module.
This allows the package to be executed with: python -m servc
"""

import asyncio
from servc.cli import cli, click


def main():
    """Main entry point for the CLI."""
    try:
        asyncio.run(cli())
    except KeyboardInterrupt:
        click.secho("Operation cancelled.", fg="red")
    except Exception as e:
        click.secho(f"{e}", fg="red")
        raise


if __name__ == "__main__":
    main()