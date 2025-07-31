# Claude Development Instructions

## Type Checking with mypy

### Setup
This project uses mypy for static type checking. The mypy dependency is included in `requirements-dev.txt`.

### Required Commands
To enable full type checking capabilities, Claude needs permission to run:
- `mypy servc --check-untyped-defs` - Run type checking on the servc package with untyped function checking
- `pip install -r requirements-dev.txt` - Install development dependencies including mypy
- `python -m pip install -r requirements-dev.txt` - Alternative pip installation method

### mypy Configuration
The project includes type stubs and typing information:
- `servc/py.typed` - Marks the package as type-aware
- Type stubs for dependencies are included in requirements-dev.txt

### Development Workflow
1. Install development dependencies: `pip install -r requirements-dev.txt`
2. Run type checking: `mypy servc --check-untyped-defs`
3. Fix any type issues before committing

## Permissions
Claude requires the following Bash tool permissions:
- `pip` commands for dependency installation
- `python` commands for running type checkers
- `mypy` commands for static type analysis