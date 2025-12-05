
import sys
import os
import logging

# Mock logging
logging.basicConfig(level=logging.INFO)

# Import Constants
try:
    import Constants
    print("Constants imported successfully")
    print(f"RESOURCE_TYPES type: {type(Constants.RESOURCE_TYPES)}")
    print(f"RESOURCE_TYPES value: {Constants.RESOURCE_TYPES}")
    print(f"LABEL_PROPERTIES type: {type(Constants.LABEL_PROPERTIES)}")
    print(f"LABEL_PROPERTIES value: {Constants.LABEL_PROPERTIES}")
    print(f"ENDPOINTS type: {type(Constants.ENDPOINTS)}")
    print(f"ENDPOINTS value: {Constants.ENDPOINTS}")
except Exception as e:
    print(f"Error importing Constants: {e}")

# Import sparql_queries
try:
    from sparql_queries import search_resources
    print("search_resources imported successfully")
except Exception as e:
    print(f"Error importing sparql_queries: {e}")

# Test search_resources
try:
    print("Testing search_resources...")
    # Simulate filter_results call
    # results_df = search_resources(query_text, limit=500, resource_type=filter_type)
    # Case 1: resource_type is None (or empty string which acts as None in search_resources logic?)
    # Wait, in app.py: filter_type = params.get('filter_type', '')
    
    print("Test 1: resource_type=''")
    search_resources("test", limit=10, resource_type="")
    print("Test 1 passed")

    print("Test 2: resource_type=None")
    search_resources("test", limit=10, resource_type=None)
    print("Test 2 passed")

except Exception as e:
    print(f"Error during test: {e}")
    import traceback
    traceback.print_exc()
