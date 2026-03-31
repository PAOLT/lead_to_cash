# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "867de34a-41e2-44a4-83ed-789a8e3feb01",
# META       "default_lakehouse_name": "ops_data",
# META       "default_lakehouse_workspace_id": "beeadc18-d85e-4c30-89e9-fa6b3fc07736",
# META       "known_lakehouses": [
# META         {
# META           "id": "867de34a-41e2-44a4-83ed-789a8e3feb01"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
import base64
import time

WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId")
ONTOLOGY_ITEM_ID = ""  
OUTPUT_PATH = "Files/ontology_definition.json"  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_ontology_definition(workspace_id, item_id):
    """
    Retrieve an ontology definition from Fabric API.
    
    Args:
        workspace_id: The workspace ID containing the ontology
        item_id: The ontology item ID
        
    Returns:
        dict: The ontology definition as a JSON object
    """
    # Get access token using notebookutils
    # Using 'pbi' audience since we're accessing Fabric API in same workspace
    # This token has Workspace.ReadWrite.All scope when running under user identity
    token = notebookutils.credentials.getToken('pbi')
    
    # Fabric API endpoint for getting item definition
    api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    # Make POST request to get the definition
    print(f"Retrieving ontology definition from workspace {workspace_id}...")
    response = requests.post(api_url, headers=headers)
    
    # Handle long-running operation (202 Accepted)
    if response.status_code == 202:
        print("Definition retrieval in progress (long-running operation)...")
        operation_id = response.headers.get('x-ms-operation-id')
        retry_after = int(response.headers.get('Retry-After', 30))
        
        # Poll for completion (simple polling logic)
        operation_url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
        max_attempts = 100
        
        for attempt in range(max_attempts):
            print(f"Waiting {retry_after} seconds before polling (attempt {attempt + 1}/{max_attempts})...")
            time.sleep(retry_after)
            
            operation_response = requests.get(operation_url, headers=headers)
            if operation_response.status_code == 200:
                # Operation completed, get the actual definition
                response = requests.get(f"{operation_url}/result", headers=headers)
                if response.status_code == 200:
                    break
            elif operation_response.status_code != 202:
                raise Exception(f"Operation polling failed: {operation_response.status_code} - {operation_response.text}")
    
    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve ontology definition: {response.status_code} - {response.text}")
    
    print("Ontology definition retrieved successfully")
    
    # Parse the response
    definition_response = response.json()
    definition_parts = definition_response.get('definition', {}).get('parts', [])
    
    # Build the complete ontology definition
    # Ontology definitions can have multiple parts (definition.json, .platform, EntityTypes, RelationshipTypes, etc.)
    ontology_def = {}
    
    for part in definition_parts:
        path = part.get('path', '')
        payload = part.get('payload', '')
        payload_type = part.get('payloadType', '')
        
        # Decode Base64 payload
        if payload_type == 'InlineBase64':
            decoded_content = base64.b64decode(payload).decode('utf-8')
            
            # Parse JSON content if it's a JSON file
            if path.endswith('.json'):
                try:
                    ontology_def[path] = json.loads(decoded_content)
                except json.JSONDecodeError:
                    # If not valid JSON, store as string
                    ontology_def[path] = decoded_content
            else:
                ontology_def[path] = decoded_content
        else:
            # Store raw payload if not Base64
            ontology_def[path] = payload
    
    return ontology_def

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_to_lakehouse(data, output_path):
    """
    Save JSON data to lakehouse.
    
    Args:
        data: The data to save (will be JSON serialized)
        output_path: The path in the lakehouse (e.g., "Files/ontology.json")
    """
    # Convert to JSON string
    json_content = json.dumps(data, indent=2)
    
    # Save to lakehouse using notebookutils.fs
    # This works for both Spark and Python notebooks
    print(f"Saving ontology definition to lakehouse at {output_path}...")
    
    # Use notebookutils.fs.put to write the file
    success = notebookutils.fs.put(output_path, json_content, overwrite=True)
    
    if success:
        print(f"Ontology definition saved successfully to {output_path}")
    else:
        raise Exception(f"Failed to save ontology definition to {output_path}")
    
    return success

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ontology_definition = get_ontology_definition(WORKSPACE_ID, ONTOLOGY_ITEM_ID)

print(f"\nOntology structure contains {len(ontology_definition)} parts:")
for part_path in ontology_definition.keys():
    print(f"  - {part_path}")

# Save to lakehouse
save_to_lakehouse(ontology_definition, OUTPUT_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
