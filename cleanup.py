import os

# List of old files to remove
files_to_remove = [
    "etl_scripts/extractor.py",
    "etl_scripts/loader.py",
    "great_expectations/expectations/bronze_suite.json",
    "great_expectations/expectations/silver_suite.json"
]

print("Starting clean-up of obsolete/renamed files...")
for f in files_to_remove:
    if os.path.exists(f):
        try:
            os.remove(f)
            print(f"Removed: {f}")
        except Exception as e:
            print(f"Failed to remove {f}: {e}")
    else:
        print(f"Not found/Already clean: {f}")
print("Clean-up complete!")
