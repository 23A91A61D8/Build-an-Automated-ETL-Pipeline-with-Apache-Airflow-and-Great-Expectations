import os
import sys
import json

def print_result(name, success, message=""):
    status = "SUCCESS" if success else "FAILED"
    color_start = "\033[92m" if success else "\033[91m"
    color_end = "\033[0m"
    print(f"[{color_start}{status}{color_end}] {name} {message}")

def check_files():
    required_files = [
        "docker-compose.yml",
        "Dockerfile.etl",
        ".env.example",
        "README.md",
        ".gitignore",
        "requirements.txt",
        "dags/ecommerce_analytics_pipeline.py",
        "etl_scripts/__init__.py",
        "etl_scripts/raw_downloader.py",
        "etl_scripts/bronze_processor.py",
        "etl_scripts/silver_transformer.py",
        "etl_scripts/validator.py",
        "etl_scripts/analytics_loader.py",
        "great_expectations/great_expectations.yml",
        "great_expectations/expectations/bronze_expectations.json",
        "great_expectations/expectations/silver_expectations.json",
        "great_expectations/checkpoints/bronze_checkpoint.yml",
        "great_expectations/checkpoints/silver_checkpoint.yml",
        "tests/__init__.py",
        "tests/test_silver_transformer.py",
        "data/raw/.gitkeep",
        "data/bronze/.gitkeep",
        "data/silver/.gitkeep"
    ]
    all_exist = True
    print("\n--- Checking File Existence ---")
    for f in required_files:
        exists = os.path.exists(f)
        print_result(f"File: {f}", exists)
        if not exists:
            all_exist = False
    return all_exist

def check_dag_syntax():
    print("\n--- Checking DAG Syntax ---")
    dag_path = "dags/ecommerce_analytics_pipeline.py"
    if not os.path.exists(dag_path):
        print_result("DAG syntax check skipped (file missing)", False)
        return False
    
    try:
        with open(dag_path, "r", encoding="utf-8") as f:
            source = f.read()
        compile(source, dag_path, "exec")
        print_result("DAG syntax is valid (compiles successfully)", True)
        return True
    except Exception as e:
        print_result("DAG has syntax errors", False, f"- Error: {str(e)}")
        return False

def check_json_syntax():
    print("\n--- Checking Great Expectations Suites JSON Syntax ---")
    suites = [
        "great_expectations/expectations/bronze_expectations.json",
        "great_expectations/expectations/silver_expectations.json"
    ]
    all_valid = True
    for s in suites:
        if not os.path.exists(s):
            print_result(f"JSON Check: {s} (file missing)", False)
            all_valid = False
            continue
        try:
            with open(s, "r", encoding="utf-8") as f:
                json.load(f)
            print_result(f"JSON Check: {s} is valid JSON", True)
        except Exception as e:
            print_result(f"JSON Check: {s} has parsing error", False, f"- Error: {str(e)}")
            all_valid = False
    return all_valid

def run_local_tests():
    print("\n--- Running Unit Tests locally ---")
    sys.path.insert(0, os.path.abspath("."))
    
    try:
        import pytest
        exit_code = pytest.main(["-v", "tests/"])
        success = (exit_code == 0)
        print_result("Unit test suite execution via Pytest", success, f"- Pytest Exit Code: {exit_code}")
        return success
    except ImportError:
        print("Pytest not installed in host Python environment. Attempting manual test run...")
        try:
            from tests.test_silver_transformer import (
                test_customer_id_imputation,
                test_negative_and_zero_value_filtering,
                test_total_price_and_aggregation
            )
            print("Running test_customer_id_imputation...")
            test_customer_id_imputation()
            print_result("test_customer_id_imputation", True)

            print("Running test_negative_and_zero_value_filtering...")
            test_negative_and_zero_value_filtering()
            print_result("test_negative_and_zero_value_filtering", True)

            print("Running test_total_price_and_aggregation...")
            test_total_price_and_aggregation()
            print_result("test_total_price_and_aggregation", True)
            
            print_result("All manual tests passed successfully!", True)
            return True
        except Exception as e:
            print_result("Manual test execution failed", False, f"- Error: {str(e)}")
            return False

if __name__ == "__main__":
    print("==============================================")
    print("ETL Pipeline Verification Script")
    print("==============================================")
    
    files_ok = check_files()
    dag_ok = check_dag_syntax()
    json_ok = check_json_syntax()
    tests_ok = run_local_tests()
    
    print("\n==============================================")
    if files_ok and dag_ok and json_ok and tests_ok:
        print_result("PROJECT VERIFICATION SUMMARY", True, "- Ready for submission!")
    else:
        print_result("PROJECT VERIFICATION SUMMARY", False, "- Please fix the issues above before submitting.")
    print("==============================================")
