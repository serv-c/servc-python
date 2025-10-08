import sys
import shutil
import subprocess
from pathlib import Path
import asyncclick as click
from pyclack.prompts import confirm, select, text, Option, spinner, note, outro, link
from pyclack.utils.styling import Color
from pyclack.core import Spinner, is_cancel

from servc.cli.config import config


def get_python():
    """Get available Python command."""
    for cmd in ["python", "python3", "py"]:
        if shutil.which(cmd):
            return cmd
    raise click.ClickException(
        "No Python interpreter found. Please install Python to proceed."
    )


PY = get_python()


PACKAGE_MANAGERS = [
    {
        "name": "uv",
        "description": "UV (Fast and Modern)",
        "note": "Recommended",
        "setup": [PY, "-m", "pip", "install", "uv"],
        "deps": [PY, "-m", "uv", "sync"],
        "run": [PY, "-m", "uv", "run", "--isolated", "worker.py"],
    },
    {
        "name": "pip",
        "description": "Pip (Standard Python Package Manager)",
        "note": "",
        "setup": None,
        "deps": [PY, "-m", "pip", "install", "-r", "requirements.txt"],
        "run": [PY, "worker.py"],
    },
    {
        "name": "poetry",
        "description": "Poetry (Dependency Management and Packaging)",
        "note": "",
        "setup": [PY, "-m", "pip", "install", "poetry"],
        "deps": [PY, "-m", "poetry", "install", "--no-root"],
        "run": [PY, "-m", "poetry", "run", "worker.py"],
    },
]


def project_name_validator(val):
    # check if the project name is empty
    if not val:
        return "Project name cannot be empty."
    # check if the project name contains spaces or special characters
    if not val.replace("-", "").replace("_", "").isalnum():
        return "Project name cannot contain spaces or special characters."
    # check if the project name is minimum 3 characters long
    if len(val) < 3:
        return "Project name must be at least 3 characters long."

    return None


async def get_project_name():
    # Prompt the user for the project name with validation
    return await text(
        message="Enter the project name",
        default_value="servc-service",
        placeholder="servc-service",
        validate=project_name_validator,
    )


async def get_pkg_manager():
    # Prompt the user for the package manager
    options = [
        Option(
            pm["name"], f"{pm['description']} {'- ' + pm['note'] if pm['note'] else ''}"
        )
        for pm in PACKAGE_MANAGERS
    ]

    return await select(
        message="Select a package manager",
        options=options,
    )


def run(cmd: list, cwd: Path, interactive: bool = False) -> None:
    """Run command in directory"""
    try:
        subprocess.run(
            cmd,
            cwd=cwd,
            check=True,
            # Show output only if interactive is True
            stdout=None if interactive else subprocess.DEVNULL,
            stderr=None if interactive else subprocess.DEVNULL,
        )
    except KeyboardInterrupt:
        if interactive:
            raise
        else:
            click.secho("\nOperation cancelled", fg="red")
            sys.exit(1)
    except subprocess.CalledProcessError as e:
        # Check if the process was likely terminated by user (Ctrl+C)
        # On Windows, Ctrl+C typically results in exit code 1 or 2
        if e.returncode in [-2, 1, 2] and interactive:
            # This is likely a user interruption, treat it as KeyboardInterrupt
            raise KeyboardInterrupt("Process terminated")
        raise click.ClickException(f"Command '{' '.join(cmd)}' failed with exit code {e.returncode}")
    except Exception as e:
        raise click.ClickException(f"Error running command '{' '.join(cmd)}': {e}")


def setup(name: str, interactive: bool = False) -> None:
    """Setup package manager in directory"""
    pm = next((pm for pm in PACKAGE_MANAGERS if pm["name"] == name), None)
    if pm is None:
        raise click.ClickException(f"Unknown Package manager '{name}'.")
    if pm["setup"]:
        run(pm["setup"], Path.cwd(), interactive)


def install(path: Path, name: str, interactive: bool = False) -> None:
    """Install project dependencies"""
    mgr = next((m for m in PACKAGE_MANAGERS if m["name"] == name), None)
    if not mgr:
        raise click.ClickException(f"Unknown package manager: {name}")
    run(mgr["deps"], path, interactive)


def start(path: Path, name: str, interactive: bool = False) -> None:
    """Start development server"""
    mgr = next((m for m in PACKAGE_MANAGERS if m["name"] == name), None)
    if not mgr:
        raise click.ClickException(f"Unknown package manager: {name}")
    try:
        run(mgr["run"], path, interactive)
    except KeyboardInterrupt:
        click.echo("\nDevelopment server stopped.")
        return


def copy_template(name: str, target: Path, project_name: str) -> None:
    """Copy template files to target"""
    template = Path(__file__).parent.parent / f"template_{name}"
    if not template.exists():
        raise click.ClickException(f"Template not found: {name}")

    # Create target directory
    target.mkdir(parents=True, exist_ok=True)

    # Copy template files
    for item in template.glob("**/*"):
        if item.is_file():
            rel_path = item.relative_to(template)
            dest = target / rel_path
            dest.parent.mkdir(parents=True, exist_ok=True)

            # Read file content and replace placeholders
            try:
                content = item.read_text()
                content = content.replace("{{PROJECT_NAME}}", project_name)
                dest.write_text(content)
            except UnicodeDecodeError:
                # If the file is not a text file, copy it as is
                shutil.copy2(item, dest)


def get_next_steps(target_path: Path, pkg_manager: str) -> str:
    """Generate next steps instructions"""
    mgr = next((m for m in PACKAGE_MANAGERS if m["name"] == pkg_manager), None)
    if not mgr:
        return ""

    rel_path = target_path.relative_to(Path.cwd())
    steps = []

    # Add cd command if not in current directory
    if target_path != Path.cwd():
        steps.append(f"cd {rel_path}")

    # Add setup command if needed
    if mgr["setup"]:
        steps.append(" ".join(mgr["setup"]))

    # Add install command
    steps.append(" ".join(mgr["deps"]))

    # Add run command
    steps.append(" ".join(mgr["run"]))

    return "\n".join(f"  $ {step}" for step in steps)


@click.command("init")
@click.argument("name", required=False)
@click.option("-t", "--template", help="Project template to use (default: pip).")
@click.option(
    "-i",
    "--immediate",
    is_flag=True,
    help="Install dependencies immediately after project creation.",
)
@click.option(
    "--interactive",
    is_flag=True,
    default=False,
    help="Show detailed setup and installation logs.",
)
@config
async def cli(ctx, name, template, immediate, interactive):
    """Initialize a new servc service project.

    Creates a new servc service with the package manager:

    \b
    Available package managers:
      uv      Fast and Modern (Recommended)
      pip     Standard Python Package Manager
      poetry  Dependency Management and Packaging

    \b
    Examples:
      # Create new service
      $ servc init my-service

      # Create service in current directory
      $ servc init .

      # Create using UV package manager
      $ servc init my-service -t uv

      # Create and start development server
      $ servc init my-service -t uv -i
    """
    ctx.log("Initializing a new servc service project...")
    try:
        # get project name
        if "." == name:
            project_name = Path.cwd().name
            target_path = Path.cwd()
        else:
            # If name is provided as argument, use it; otherwise, prompt for it
            if name:
                error_msg = project_name_validator(name)
                if error_msg:
                    name = None
                    raise click.secho(f"{error_msg}", fg="yellow")
                else:
                    project_name = name
            if not name:
                # prompt for project name
                project_name = await get_project_name()
                if is_cancel(project_name):
                    raise KeyboardInterrupt("Operation cancelled.")

            target_path = Path.cwd() / project_name
        # Handle package manager selection
        if template:
            if template not in [pm["name"] for pm in PACKAGE_MANAGERS]:
                ctx.log(f"Invalid template '{template}'. Using 'pip' instead.")
                pkg_manager = "pip"
            else:
                pkg_manager = template
        else:
            pkg_manager = await get_pkg_manager()
            if is_cancel(pkg_manager):
                raise KeyboardInterrupt("Operation cancelled.")

        # check if target path already exists and is not empty
        if target_path.exists() and any(target_path.iterdir()):
            # prompt user to choose how to proceed
            res = await select(
                message=f"Target directory '{target_path}' already exists and is not empty. How do you want to proceed?",
                options=[
                    Option("continue", "Continue and merge with existing files"),
                    Option("overwrite", "Overwrite the existing directory"),
                    Option("cancel", "Cancel the operation"),
                ],
            )
            if res == "cancel" or is_cancel(res):
                raise KeyboardInterrupt("Operation cancelled.")
            elif res == "overwrite":
                shutil.rmtree(target_path)
                ctx.log(f"Overwritten existing directory.")
            else:
                ctx.log(f"Merging with existing files.")
        # Create project structure based on inputs
        copy_template(pkg_manager, target_path, project_name)
        ctx.log(f"Project '{project_name}' created.")
        # Install dependencies if immediate flag is set
        if not immediate:
            # prompt user to install dependencies now by yes/no
            immediate = await confirm("Do you want to install dependencies now?")
            if is_cancel(immediate):
                raise KeyboardInterrupt("Operation cancelled.")

        if immediate:
            async with spinner("Setting up project...") as spin:
                try:
                    # 0. Setup package manager if needed
                    spin.update(f"Setting up {pkg_manager}...")
                    setup(pkg_manager, interactive)
                    spin.update("Installing dependencies...")
                    # 1. Install dependencies
                    install(target_path, pkg_manager, interactive)
                    spin.stop("Dependencies installed successfully.", code=0)
                except KeyboardInterrupt:
                    spin.stop("\nSetup cancelled", code=1)
                    sys.exit(1)
            
            # Start development server
            try:
                click.secho("Starting development server...", fg="green")
                start(target_path, pkg_manager, True)
            except KeyboardInterrupt:
                click.secho("Development server stopped.", fg="yellow")
                return
        else:
            steps = get_next_steps(target_path, pkg_manager)
            note(title="Next steps", message="\nRun the following commands:\n" + steps)
            outro(
                f"{Color.dim(f'Problems? {link(url='https://github.com/serv-c/servc-python')}')}"
            )
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        click.secho(f"{e}", fg="red")
        sys.exit(1)
