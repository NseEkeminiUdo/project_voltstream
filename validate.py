#!/usr/bin/env python3
"""
Pre-Push Validation Script for project_voltstream

Run this before pushing to ensure all checks pass:
    python validate.py

Or make it executable:
    chmod +x validate.py
    ./validate.py
"""

import sys
import ast
from pathlib import Path

# Color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


class ValidationRunner:
    def __init__(self, project_root):
        self.project_root = Path(project_root)
        self.passed = []
        self.failed = []
        self.warnings = []

    def print_header(self, text):
        print(f"\n{BLUE}{'=' * 70}{RESET}")
        print(f"{BLUE}  {text}{RESET}")
        print(f"{BLUE}{'=' * 70}{RESET}\n")

    def check_python_syntax(self):
        """Validate Python syntax for all .py files"""
        self.print_header("1. CHECKING PYTHON SYNTAX")

        py_files = [
            "health_check/health_check.py",
            "utils/bronze.py",
            "utils/gold.py",
            "utils/shared.py",
            "utils/silver.py"
        ]

        all_valid = True
        for rel_path in py_files:
            filepath = self.project_root / rel_path
            if not filepath.exists():
                print(f"{YELLOW}⚠ SKIP{RESET} {rel_path:40s} (not found)")
                self.warnings.append(f"{rel_path} not found")
                continue

            try:
                with open(filepath, 'r') as f:
                    ast.parse(f.read())
                print(f"{GREEN}✓ PASS{RESET} {rel_path:40s}")
                self.passed.append(f"Syntax: {rel_path}")
            except SyntaxError as e:
                print(f"{RED}✗ FAIL{RESET} {rel_path:40s} Line {e.lineno}: {e.msg}")
                self.failed.append(f"Syntax error in {rel_path}")
                all_valid = False

        return all_valid

    def check_dbutils_usage(self):
        """Check for proper dbutils usage (should use wrapper functions)"""
        self.print_header("2. CHECKING DBUTILS USAGE")

        files = list(self.project_root.rglob('*.py'))
        files = [f for f in files if '__pycache__' not in str(
            f) and '.git' not in str(f)]

        # Patterns that should NOT be used directly
        bad_patterns = [
            'dbutils.notebook.entry_point',  # Should use spark.conf
            'dbutils.secrets.get(',          # Should use get_secret()
            'dbutils.jobs.taskValues.set(',  # Should use set_task_value()
        ]

        # Files that are allowed to have direct dbutils access (the wrapper module)
        allowed_files = ['utils/shared.py', 'validate.py']

        all_clean = True
        for filepath in files:
            rel_path = filepath.relative_to(self.project_root)

            # Skip allowed files
            if str(rel_path) in allowed_files:
                print(f"{BLUE}• SKIP{RESET} {str(rel_path):40s} (wrapper module)")
                continue

            with open(filepath, 'r') as f:
                content = f.read()

            found_issues = []
            for pattern in bad_patterns:
                if pattern in content:
                    found_issues.append(pattern)

            if found_issues:
                print(f"{RED}✗ FAIL{RESET} {str(rel_path):40s}")
                for issue in found_issues:
                    print(f"         Found: {issue}")
                self.failed.append(f"Direct dbutils usage in {rel_path}")
                all_clean = False
            else:
                print(f"{GREEN}✓ PASS{RESET} {str(rel_path):40s}")
                self.passed.append(f"Proper dbutils usage: {rel_path}")

        return all_clean

    def check_imports(self):
        """Test that key modules can be imported"""
        self.print_header("3. CHECKING MODULE IMPORTS")

        # Add project to path
        sys.path.insert(0, str(self.project_root))

        modules_to_test = [
            ("utils.shared", "get_run_context"),
            ("utils.shared", "get_secret"),
            ("utils.shared", "set_task_value"),
            ("utils.bronze", None),
            ("utils.silver", None),
            ("utils.gold", None)
        ]

        all_imported = True
        for module_name, function_name in modules_to_test:
            try:
                module = __import__(module_name, fromlist=[''])
                if function_name and not hasattr(module, function_name):
                    print(
                        f"{RED}✗ FAIL{RESET} {module_name:40s} (missing {function_name})")
                    self.failed.append(
                        f"Missing function {function_name} in {module_name}")
                    all_imported = False
                else:
                    display = f"{module_name}.{function_name}" if function_name else module_name
                    print(f"{GREEN}✓ PASS{RESET} {display:40s}")
                    self.passed.append(f"Import: {display}")
            except Exception as e:
                print(f"{RED}✗ FAIL{RESET} {module_name:40s} ({str(e)[:30]})")
                self.failed.append(f"Import error: {module_name}")
                all_imported = False

        return all_imported

    def check_notebook_paths(self):
        """Verify that notebook files exist"""
        self.print_header("4. CHECKING NOTEBOOK PATHS")

        notebooks = [
            "notebooks/bronze/extract_station_data.ipynb",
            "notebooks/bronze/extract_weather_data.ipynb",
            "notebooks/silver/process_station_data.ipynb",
            "notebooks/silver/process_connector_data.ipynb",
            "notebooks/silver/process_weather_data.ipynb",
            "notebooks/gold/join_station_and_weather_data.ipynb"
        ]

        all_exist = True
        for rel_path in notebooks:
            filepath = self.project_root / rel_path
            if filepath.exists():
                print(f"{GREEN}✓ PASS{RESET} {rel_path:40s}")
                self.passed.append(f"Exists: {rel_path}")
            else:
                print(f"{RED}✗ FAIL{RESET} {rel_path:40s} (not found)")
                self.failed.append(f"Missing: {rel_path}")
                all_exist = False

        return all_exist

    def check_path_typos(self):
        """Check for common path typos"""
        self.print_header("5. CHECKING FOR PATH TYPOS")

        # Check if the typo directory exists
        typo_path = self.project_root.parent / "project_volltstream"

        if typo_path.exists():
            print(f"{YELLOW}⚠ WARN{RESET} Found typo directory: project_volltstream")
            print(f"You may want to delete it to avoid confusion")
            self.warnings.append("Typo directory exists: project_volltstream")
        else:
            print(f"{GREEN}✓ PASS{RESET} No typo directories found")
            self.passed.append("No path typos")

        return True

    def print_summary(self):
        """Print final summary"""
        self.print_header("VALIDATION SUMMARY")

        total = len(self.passed) + len(self.failed)

        print(f"{GREEN}Passed:   {len(self.passed)}{RESET}")
        print(f"{RED}Failed:   {len(self.failed)}{RESET}")
        print(f"{YELLOW}Warnings: {len(self.warnings)}{RESET}")
        print(f"Total:    {total}")
        print()

        if self.failed:
            print(f"{RED}{'=' * 70}{RESET}")
            print(f"{RED}  ❌ VALIDATION FAILED - DO NOT PUSH{RESET}")
            print(f"{RED}{'=' * 70}{RESET}")
            print("\nFailed checks:")
            for fail in self.failed:
                print(f"  • {fail}")
            return False
        else:
            print(f"{GREEN}{'=' * 70}{RESET}")
            print(f"{GREEN}  ✅ ALL CHECKS PASSED - SAFE TO PUSH{RESET}")
            print(f"{GREEN}{'=' * 70}{RESET}")
            return True

    def run_all(self):
        """Run all validation checks"""
        checks = [
            self.check_python_syntax(),
            self.check_dbutils_usage(),
            self.check_imports(),
            self.check_notebook_paths(),
            self.check_path_typos()
        ]

        self.print_summary()
        return all(checks)


if __name__ == "__main__":
    # Determine project root
    script_dir = Path(__file__).parent
    project_root = script_dir

    print(f"{BLUE}Project Root: {project_root}{RESET}")

    runner = ValidationRunner(project_root)
    success = runner.run_all()

    sys.exit(0 if success else 1)
