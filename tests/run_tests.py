#!/usr/bin/env python3
"""
Test Runner for project_voltstream

This script runs all tests for the project_voltstream data pipeline using pytest.
It provides formatted output, test summary, and exit codes for CI/CD integration.

Usage:
    python run_tests.py [options]

Options:
    --verbose, -v        Run tests with verbose output
    --unit-only          Run only unit tests (skip integration tests)
    --integration-only   Run only integration tests
    --coverage           Run with coverage report (requires pytest-cov)
    --failfast, -x       Stop on first test failure
    --markers, -m        Run tests matching specific markers (e.g., -m "not slow")

Additional pytest options can be passed directly to pytest.

Examples:
    python run_tests.py                    # Run all tests
    python run_tests.py -v                 # Run with verbose output
    python run_tests.py --unit-only        # Run only unit tests
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py -x                 # Stop on first failure
    python run_tests.py -m "not slow"      # Skip slow tests
"""

import sys
import os
import subprocess
import argparse
from datetime import datetime
from pathlib import Path


def print_banner(text):
    """Print a formatted banner"""
    print("\n" + "="*70)
    print(f"  {text}")
    print("="*70 + "\n")


def check_pytest_installed():
    """Check if pytest is installed"""
    try:
        import pytest
        return True
    except ImportError:
        return False


def pip_install(pkg):
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])


def run_tests(args):
    """Run tests using pytest"""
    print("Installing dependencies...")
    pip_install("geopandas")
    if not check_pytest_installed():
        print("\033[91mError: pytest not installed.\033[0m")
        print("Install with: pip install pytest")
        return 2

    start_time = datetime.now()

    # Print header
    print_banner("project_voltstream Test Suite (pytest)")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python version: {sys.version.split()[0]}")
    print()

    # Build pytest command
    pytest_args = []

    # Select test scope
    if args.unit_only:
        print("Running UNIT TESTS only...\n")
        pytest_args.extend([
            'tests/test_shared.py',
            'tests/test_bronze.py',
            'tests/test_silver.py',
            'tests/test_gold.py'
        ])
    elif args.integration_only:
        print("Running INTEGRATION TESTS only...\n")
        pytest_args.append('tests/test_integration.py')
    else:
        print("Running ALL TESTS...\n")
        pytest_args.append('tests/')

    # Add verbosity
    if args.verbose:
        pytest_args.append('-v')

    # Add failfast
    if args.failfast:
        pytest_args.append('-x')

    # Add markers
    if args.markers:
        pytest_args.extend(['-m', args.markers])

    # Add coverage
    if args.coverage:
        if not check_coverage_installed():
            print("\033[91mError: pytest-cov not installed.\033[0m")
            print("Install with: pip install pytest-cov")
            return 2

        pytest_args.extend([
            '--cov=utils',
            '--cov-report=html',
            '--cov-report=term-missing'
        ])

    # Add color output
    pytest_args.append('--color=yes')

    # Get project root directory (parent of tests/ directory)
    project_root = Path(__file__).parent.parent.resolve()

    # Run pytest using the current Python interpreter
    exit_code = 2  # Default to error code
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", *pytest_args],
            cwd=project_root,
            capture_output=False,  # Let output stream to console
            text=True,
            check=False  # Don't raise exception on non-zero exit
        )
        exit_code = result.returncode

    except FileNotFoundError as e:
        print("\033[91mError: Python executable not found.\033[0m")
        print("This should not happen. Ensure your environment is correctly configured.")
        print(f"sys.executable: {sys.executable}")
        print(f"Error details: {e}")
        exit_code = 2

    except (OSError, subprocess.SubprocessError) as e:
        print(f"\033[91mError running pytest: {e}\033[0m")
        print("This may be a system or subprocess error.")
        exit_code = 2

    except Exception as e:
        # Catch any unexpected exceptions
        print(f"\033[91mUnexpected error running pytest: {e}\033[0m")
        import traceback
        traceback.print_exc()
        exit_code = 2

    # Print duration
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\nTotal duration: {duration:.2f}s")

    return exit_code


def check_coverage_installed():
    """Check if pytest-cov is installed"""
    try:
        import pytest_cov
        return True
    except ImportError:
        return False


def main():
    """Main execution function"""
    sys
    parser = argparse.ArgumentParser(
        description='Run project_voltstream test suite with pytest',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py                    # Run all tests
  python run_tests.py -v                 # Run with verbose output
  python run_tests.py --unit-only        # Run only unit tests
  python run_tests.py --coverage         # Run with coverage report
  python run_tests.py -x                 # Stop on first failure
  python run_tests.py -m "not slow"      # Skip slow tests
        """
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Run tests with verbose output'
    )

    parser.add_argument(
        '--unit-only',
        action='store_true',
        help='Run only unit tests (skip integration tests)'
    )

    parser.add_argument(
        '--integration-only',
        action='store_true',
        help='Run only integration tests'
    )

    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run with coverage report (requires pytest-cov)'
    )

    parser.add_argument(
        '-x', '--failfast',
        action='store_true',
        help='Stop on first test failure'
    )

    parser.add_argument(
        '-m', '--markers',
        type=str,
        help='Run tests matching specific markers (e.g., "not slow")'
    )

    args = parser.parse_args()

    # Validate argument combinations
    if args.unit_only and args.integration_only:
        print(
            "\033[91mError: Cannot specify both --unit-only and --integration-only\033[0m")
        return 2

    try:
        exit_code = run_tests(args)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nTest run interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n\033[91mError running tests: {e}\033[0m")
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
