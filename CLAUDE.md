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

## Unit Testing with unittest

### Setup
This project uses Python's built-in unittest framework for testing. The coverage dependency is included in `requirements-dev.txt` for test coverage analysis.

### Required Commands
To enable full unit testing capabilities, Claude needs permission to run:
- `python -m unittest discover tests` - Run all unit tests in the tests directory
- `python -m unittest tests.test_config` - Run specific test module
- `python -m coverage run -m unittest discover tests` - Run tests with coverage analysis
- `python -m coverage report` - Display coverage report
- `python -m coverage html` - Generate HTML coverage report

### Test Structure
The project includes comprehensive unit tests:
- `tests/` directory contains all test files
- Test files follow the `test_*.py` naming convention
- Uses standard unittest.TestCase classes for test organization

### Development Workflow
1. Install development dependencies: `pip install -r requirements-dev.txt`
2. Run all tests: `python -m unittest discover tests`
3. Run tests with coverage: `python -m coverage run -m unittest discover tests`
4. Check coverage report: `python -m coverage report`
5. Fix any failing tests before committing

## Permissions
Claude requires the following Bash tool permissions:
- `pip` commands for dependency installation
- `python` commands for running type checkers and unit tests
- `mypy` commands for static type analysis