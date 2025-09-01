from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import sqlite3
import json
from datetime import datetime
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, ValidationError
import uvicorn
import oracledb
import os
import re
import logging
from contextlib import contextmanager
from builder import build_es_query_v3, build_es_query_v2
import re
import json
import logging
from typing import List, Dict, Any, Tuple
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
from dataload import OracleElasticsearchMapper, map_oracle_to_elastic
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from aielastic import convert_query_to_questions, validate_elasticsearch_mapping, ElasticsearchQueryRequest, \
    ElasticsearchMappingRequest, MappingValidationResponse


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Oracle to Elasticsearch Mapping Generator & Elasticsearch Mapping Builder")


# Custom exception handler to ensure JSON responses
@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

class FormConfiguration(BaseModel):
    id: Optional[int] = None
    name: str
    url: str
    environment: int
    index: str
    fields: Dict[str, Any]
    created_at: Optional[str] = None

# Database models
class ElasticsearchEnvironment(BaseModel):
    id: Optional[int] = None
    name: str
    host_url: str
    username: Optional[str] = None
    password: Optional[str] = None

class OracleEnvironment(BaseModel):
    id: Optional[int] = None
    name: str
    url: str
    username: str
    password: str



class IndexMapping(BaseModel):
    id: Optional[int] = None
    env_id: int
    index_name: str
    mapping_name: str
    mapping_json: str


from typing import Optional, List, Dict, Any
from pydantic import BaseModel, model_validator, Field

class MappingField(BaseModel):
    field_name: str
    field_type: Optional[str] = None
    elastic_type: Optional[str] = None
    oracle_type: Optional[str] = Field(default=None)  # Explicitly use Field
    ui_component_type: str = "text_box"
    is_nested: bool = False
    parent_field: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    nested_fields: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    source_index: Optional[str] = None
    key_field: Optional[str] = None
    value_field: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def set_field_type_from_elastic_type(cls, values):
        if not values.get("field_type") and values.get("elastic_type"):
            values["field_type"] = values["elastic_type"]
        return values

# Alternative: Use model_config to allow extra fields and make validation more flexible
class MappingFieldFlexible(BaseModel):
    model_config = {"extra": "ignore"}  # Ignore extra fields

    field_name: str
    field_type: Optional[str] = None
    elastic_type: Optional[str] = None
    oracle_type: Optional[str] = None
    ui_component_type: str = "text_box"
    is_nested: bool = False
    parent_field: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    nested_fields: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    source_index: Optional[str] = None
    key_field: Optional[str] = None
    value_field: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def set_field_type_from_elastic_type(cls, values):
        if not values.get("field_type") and values.get("elastic_type"):
            values["field_type"] = values["elastic_type"]
        return values


class MappingUpdate(BaseModel):
    env_id: int
    index_name: str
    root_fields: List[str] = Field(default_factory=list)
    parent_child_fields: List[str] = Field(default_factory=list)
    parent_child_relation: Optional[str] = None
    nested_fields: List[str] = Field(default_factory=list)
    ai_fields: List[str] = Field(default_factory=list)









# Database initialization
def init_db():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Create Elasticsearch environments table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS elasticsearch_environments (
                                                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                             name TEXT UNIQUE NOT NULL,
                                                                             host_url TEXT NOT NULL,
                                                                             username TEXT,
                                                                             password TEXT,
                                                                             created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                   )
                   ''')

    # Create Oracle environments table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS oracle_environments (
                                                                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                      name TEXT UNIQUE NOT NULL,
                                                                      url TEXT NOT NULL,
                                                                      username TEXT NOT NULL,
                                                                      password TEXT NOT NULL,
                                                                      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                   )
                   ''')

    # Create index mappings table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS index_mappings (
                                                                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                 env_id INTEGER NOT NULL,
                                                                 index_name TEXT NOT NULL,
                                                                 mapping_name TEXT NOT NULL,
                                                                 mapping_json TEXT NOT NULL,
                                                                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                 FOREIGN KEY (env_id) REFERENCES elasticsearch_environments (id)
                       )
                   ''')

    # Create form configurations table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS form_configurations (
                                                                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                      name TEXT NOT NULL,
                                                                      url TEXT UNIQUE NOT NULL,
                                                                      environment INTEGER NOT NULL,
                                                                      index_name TEXT NOT NULL,
                                                                      fields_json TEXT NOT NULL,
                                                                      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                      FOREIGN KEY (environment) REFERENCES elasticsearch_environments (id)
                       )
                   ''')
    conn.commit()
    conn.close()


def save_form_configuration(form_config: FormConfiguration):
    """Save form configuration to database"""
    with get_db() as conn:
        cursor = conn.cursor()

        if form_config.id:
            cursor.execute(
                "UPDATE form_configurations SET name=?, url=?, environment=?, index_name=?, fields_json=? WHERE id=?",
                (form_config.name, form_config.url, form_config.environment, form_config.index,
                 json.dumps(form_config.fields), form_config.id)
            )
        else:
            cursor.execute(
                "INSERT INTO form_configurations (name, url, environment, index_name, fields_json) VALUES (?, ?, ?, ?, ?)",
                (form_config.name, form_config.url, form_config.environment, form_config.index,
                 json.dumps(form_config.fields))
            )

        conn.commit()
        return cursor.lastrowid

def get_form_configurations():
    """Get all form configurations"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT f.*, e.name as environment_name
                       FROM form_configurations f
                                JOIN elasticsearch_environments e ON f.environment = e.id
                       ORDER BY f.created_at DESC
                       """)
        return [dict(row) for row in cursor.fetchall()]

def get_form_configuration_by_url(url: str):
    """Get form configuration by URL"""
    with get_db() as conn:
        cursor = conn.cursor()
        print(url)
        cursor.execute("""
                       SELECT f.*, e.name as environment_name, e.host_url, e.username, e.password
                       FROM form_configurations f
                                JOIN elasticsearch_environments e ON f.environment = e.id
                       WHERE f.url = ?
                       """, (url,))
        result = cursor.fetchone()
        print(result)
        if result:
            form_data = dict(result)
            form_data['fields'] = json.loads(form_data['fields_json'])
            return form_data
        return None

def delete_form_configuration(form_id: int):
    """Delete form configuration"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM form_configurations WHERE id=?", (form_id,))
        conn.commit()
        return cursor.rowcount > 0

# Oracle database functions
def test_oracle_connection(url: str, username: str, password: str):
    """Test connection to Oracle database"""
    try:
        import oracledb

        # Parse connection string
        if '@' in url:
            connection_parts = url.split('@')
            dsn = connection_parts[1] if len(connection_parts) == 2 else url
        else:
            dsn = url

        with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            return {"success": True, "message": "Connection successful"}

    except ImportError:
        return {"success": False, "message": "oracledb module not installed"}
    except Exception as e:
        return {"success": False, "message": f"Connection failed: {str(e)}"}

def get_oracle_tables(url: str, username: str, password: str):
    """Get list of tables from Oracle database"""
    try:
        import oracledb

        if '@' in url:
            connection_parts = url.split('@')
            dsn = connection_parts[1] if len(connection_parts) == 2 else url
        else:
            dsn = url

        with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            table_names = [row[0] for row in cursor.fetchall()]
            # Convert to objects with table_name property for frontend compatibility
            tables = [{"table_name": name} for name in table_names]
            return {"success": True, "tables": tables}

    except Exception as e:
        return {"success": False, "message": str(e), "tables": []}

def get_table_columns(url: str, username: str, password: str, table_name: str):
    """Get columns for a specific Oracle table"""
    try:
        import oracledb

        if '@' in url:
            connection_parts = url.split('@')
            dsn = connection_parts[1] if len(connection_parts) == 2 else url
        else:
            dsn = url

        with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
            cursor = connection.cursor()
            cursor.execute("""
                           SELECT column_name, data_type, data_length, nullable
                           FROM user_tab_columns
                           WHERE table_name = UPPER(:1)
                           ORDER BY column_id
                           """, (table_name,))

            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'length': row[2],
                    'nullable': row[3] == 'Y'
                })

            return {"success": True, "columns": columns}

    except Exception as e:
        return {"success": False, "message": str(e), "columns": []}

def oracle_to_elastic_type(oracle_type: str) -> str:
    """Convert Oracle data types to Elasticsearch types"""
    oracle_type = oracle_type.upper()

    if oracle_type in ['NUMBER', 'INTEGER', 'INT']:
        return 'long'
    elif oracle_type in ['VARCHAR2', 'VARCHAR', 'NVARCHAR2', 'CLOB', 'TEXT']:
        return 'text'
    elif oracle_type in ['CHAR', 'NCHAR'] or oracle_type.startswith('CHAR('):
        return 'keyword'
    elif oracle_type in ['DATE', 'TIMESTAMP'] or oracle_type.startswith('TIMESTAMP'):
        return 'date' if oracle_type == 'DATE' else 'text'  # Handle TIMESTAMP(6) as text for now
    elif oracle_type in ['FLOAT', 'DOUBLE', 'REAL']:
        return 'double'
    elif oracle_type == 'BOOLEAN':
        return 'boolean'
    else:
        return 'text'

# Elasticsearch connection functions
def test_elasticsearch_connection(host_url: str, username: Optional[str] = None, password: Optional[str] = None):
    """Test connection to Elasticsearch cluster"""
    try:
        # Clean up URL format
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        # Prepare auth
        auth = None
        if username and password:
            auth = (username, password)

        # Test connection with cluster health endpoint
        response = requests.get(
            f"{host_url}/_cluster/health",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            health_data = response.json()
            return {
                "success": True,
                "message": f"Connected successfully to {health_data.get('cluster_name', 'cluster')}",
                "cluster_name": health_data.get('cluster_name'),
                "status": health_data.get('status'),
                "number_of_nodes": health_data.get('number_of_nodes')
            }
        else:
            return {
                "success": False,
                "message": f"Connection failed with status {response.status_code}: {response.text}"
            }

    except requests.exceptions.ConnectionError:
        return {"success": False, "message": "Could not connect to Elasticsearch. Check URL and network connectivity."}
    except requests.exceptions.Timeout:
        return {"success": False, "message": "Connection timeout. Elasticsearch may be unreachable."}
    except Exception as e:
        return {"success": False, "message": f"Connection error: {str(e)}"}

def get_elasticsearch_indices(host_url: str, username: Optional[str] = None, password: Optional[str] = None):
    """Get all indices from Elasticsearch cluster"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        response = requests.get(
            f"{host_url}/_cat/indices?format=json&h=index,docs.count,store.size",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch indices: {response.status_code}")

    except Exception as e:
        raise Exception(f"Error fetching indices: {str(e)}")

def get_elasticsearch_mapping(host_url: str, index_name: str, username: Optional[str] = None, password: Optional[str] = None):
    """Get mapping for a specific index"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        mapping_url = f"{host_url}/{index_name}/_mapping"
        print(f"DEBUG: Making request to {mapping_url}")

        response = requests.get(
            mapping_url,
            auth=auth,
            timeout=10,
            verify=False
        )

        print(f"DEBUG: Response status: {response.status_code}")

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise Exception(f"Index '{index_name}' not found (404)")
        elif response.status_code == 401:
            raise Exception(f"Authentication failed (401)")
        elif response.status_code == 403:
            raise Exception(f"Access forbidden (403)")
        else:
            response_text = response.text[:200] if response.text else "No response content"
            raise Exception(f"Failed to fetch mapping: HTTP {response.status_code} - {response_text}")

    except requests.exceptions.ConnectionError as e:
        raise Exception(f"Connection refused - Elasticsearch server may not be running: {str(e)}")
    except requests.exceptions.Timeout as e:
        raise Exception(f"Request timeout - Elasticsearch server may be slow or unresponsive: {str(e)}")
    except Exception as e:
        raise Exception(f"Error fetching mapping: {str(e)}")

def create_elasticsearch_index(host_url: str, index_name: str, mapping: dict, username: Optional[str] = None, password: Optional[str] = None):
    """Create a new index with mapping in Elasticsearch"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        # Extract just the properties from the nested mapping structure
        if "mappings" in mapping and "properties" in mapping["mappings"]:
            properties = mapping["mappings"]["properties"]
        elif "properties" in mapping:
            properties = mapping["properties"]
        else:
            properties = mapping

        # Create the correct structure
        index_body = {
            "mappings": {
                "properties": properties
            }
        }

        print("=== FINAL INDEX BODY ===")
        import json
        print(json.dumps(index_body, indent=2))
        print("=== END ===")

        response = requests.put(
            f"{host_url}/{index_name}",
            auth=auth,
            json=index_body,
            headers={'Content-Type': 'application/json'},
            timeout=30,
            verify=False
        )

        if response.status_code in [200, 201]:
            return {
                "success": True,
                "message": f"Index '{index_name}' created successfully",
                "response": response.json()
            }
        else:
            return {
                "success": False,
                "message": f"Failed to create index: {response.status_code} - {response.text}"
            }

    except Exception as e:
        return {
            "success": False,
            "message": f"Error creating index: {str(e)}"
        }



def create_elasticsearch_index_v2(host_url: str, index_name: str, mapping: dict, username: Optional[str] = None, password: Optional[str] = None):
    """Create a new index with mapping in Elasticsearch"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        # Prepare the index creation request
        index_body = {
            "mappings": mapping
        }
        print(mapping)
        response = requests.put(
            f"{host_url}/{index_name}",
            auth=auth,
            json=mapping,
            timeout=30,
            verify=False
        )

        if response.status_code in [200, 201]:
            return {
                "success": True,
                "message": f"Index '{index_name}' created successfully",
                "response": response.json()
            }
        else:
            return {
                "success": False,
                "message": f"Failed to create index: {response.status_code} - {response.text}"
            }

    except Exception as e:
        return {
            "success": False,
            "message": f"Error creating index: {str(e)}"
        }

# Elasticsearch mapping generation helpers
def get_default_field_mapping(field_type: str) -> dict:
    """Get default mapping configuration for field types"""
    default_mappings = {
        'text': {
            'type': 'text',
            'fields': {
                'keyword': {
                    'type': 'keyword',
                    'ignore_above': 256
                }
            }
        },
        'keyword': {'type': 'keyword'},
        'long': {'type': 'long'},
        'integer': {'type': 'integer'},
        'double': {'type': 'double'},
        'float': {'type': 'float'},
        'date': {'type': 'date'},
        'boolean': {'type': 'boolean'},
        'binary': {'type': 'binary'},
        'nested': {'type': 'nested'},
        'object': {'type': 'object'},
        'join': {'type': 'join'},
        'dense_vector': {'type': 'dense_vector'},
        'sparse_vector': {'type': 'sparse_vector'}
    }
    return default_mappings.get(field_type, {'type': 'text'})

def generate_elasticsearch_mapping(mapping_fields: List[MappingField]) -> dict:
    """Generate Elasticsearch mapping from user-defined fields"""
    properties = {}

    for field in mapping_fields:
        field_def = get_default_field_mapping(field.field_type)

        # Apply custom properties
        if field.properties:
            field_def.update(field.properties)

        # Handle nested fields
        if field.nested_fields:
            if field.field_type in ['nested', 'object']:
                field_def['properties'] = {}
                for nested_field in field.nested_fields:
                    nested_def = get_default_field_mapping(nested_field.get('type', 'text'))
                    if nested_field.get('properties'):
                        nested_def.update(nested_field['properties'])
                    field_def['properties'][nested_field['name']] = nested_def

        properties[field.field_name] = field_def

    return {"properties": properties}

# API Routes
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    environments = get_environments()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "environments": environments
    })

@app.post("/environments/elasticsearch")
async def create_elasticsearch_environment(
        name: str = Form(...),
        host_url: str = Form(...),
        username: str = Form(None),
        password: str = Form(None)
):
    env = ElasticsearchEnvironment(name=name, host_url=host_url, username=username, password=password)
    env_id = save_environment(env)
    return JSONResponse({"success": True, "id": env_id, "type": "elasticsearch"})

@app.post("/environments/oracle")
async def create_oracle_environment(
        name: str = Form(...),
        url: str = Form(...),
        username: str = Form(...),
        password: str = Form(...)
):
    env = OracleEnvironment(name=name, url=url, username=username, password=password)
    env_id = save_environment(env)
    return JSONResponse({"success": True, "id": env_id, "type": "oracle"})

@app.get("/environments")
async def list_environments():
    return get_environments()

@app.delete("/environments/{env_type}/{env_id}")
async def remove_environment(env_id: int, env_type: str):
    delete_environment(env_id, env_type)
    return JSONResponse({"success": True})



@app.get("/indices/{env_id}")
async def list_indices(env_id: int):
    environments = get_elasticsearch_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    indices = get_elasticsearch_indices(env['host_url'], env.get('username'), env.get('password'))
    return indices

@app.get("/tables/{env_id}")
async def list_tables(env_id: int):
    environments = get_oracle_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    tables = get_oracle_tables(env['url'], env['username'], env['password'])
    return tables

@app.get("/columns/{env_id}/{table_name}")
async def list_columns(env_id: int, table_name: str):
    environments = get_oracle_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    columns = get_table_columns(env['url'], env['username'], env['password'], table_name)
    return columns

@app.get("/mapping/{env_id}/{index_name}")
async def get_index_mapping(env_id: int, index_name: str):
    try:
        print(f"DEBUG: Starting mapping request for env_id={env_id}, index_name='{index_name}'")

        # Get environments with proper handling of both return formats
        raw_environments = get_elasticsearch_environments()
        print(f"DEBUG: Raw environments response: {raw_environments}")
        print(f"DEBUG: Raw environments type: {type(raw_environments)}")

        # Handle both possible return formats
        if isinstance(raw_environments, dict) and 'elasticsearch' in raw_environments:
            print("DEBUG: Detected nested environment structure, extracting elasticsearch environments")
            environments = raw_environments['elasticsearch']
        elif isinstance(raw_environments, list):
            print("DEBUG: Using direct list of elasticsearch environments")
            environments = raw_environments
        else:
            print(f"ERROR: Unexpected environments format: {type(raw_environments)}")
            raise HTTPException(status_code=500, detail="Database error: Unexpected environment data format")

        print(f"DEBUG: Processing {len(environments)} environments for lookup")
        if environments:
            print(f"DEBUG: First environment structure: {environments[0]}")

        # Find environment with robust error handling
        env = None
        for e in environments:
            print(f"DEBUG: Checking environment: {type(e)}, content: {e}")
            if isinstance(e, dict) and 'id' in e and e['id'] == env_id:
                env = e
                break
            elif isinstance(e, str):
                print(f"ERROR: Environment is string instead of dict: {e}")
                raise HTTPException(status_code=500, detail="Database error: Invalid environment data format")

        if not env:
            raise HTTPException(status_code=404, detail=f"Elasticsearch environment with ID {env_id} not found")

        # Log the request details for debugging
        print(f"DEBUG: Found environment: {env}")
        print(f"DEBUG: Fetching mapping for index '{index_name}' from environment ID {env_id}")
        print(f"DEBUG: Elasticsearch URL: {env['host_url']}")

        mapping = get_elasticsearch_mapping(env['host_url'], index_name, env.get('username'), env.get('password'))
        print(f"DEBUG: Successfully retrieved mapping for '{index_name}'")
        return {"mapping": mapping}

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        error_msg = str(e)
        print(f"ERROR: Failed to retrieve mapping for '{index_name}': {error_msg}")
        print(f"ERROR: Exception type: {type(e)}")

        # Check for the specific TypeError
        if "string indices must be integers, not 'str'" in error_msg:
            raise HTTPException(status_code=500, detail="Database error: Environment data is corrupted. Please check the database or recreate the environment.")

        # Check for common connection issues
        if "Connection refused" in error_msg or "Failed to establish" in error_msg:
            raise HTTPException(status_code=503, detail=f"Cannot connect to Elasticsearch. Please verify the server is running and accessible.")
        elif "404" in error_msg:
            raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found in Elasticsearch cluster")
        elif "401" in error_msg or "403" in error_msg:
            raise HTTPException(status_code=401, detail=f"Authentication failed for Elasticsearch cluster")
        else:
            raise HTTPException(status_code=500, detail=f"Error retrieving mapping for '{index_name}': {error_msg}")

@app.post("/create-index/{env_id}")
async def create_index(
        env_id: int,
        index_name: str = Form(...),
        mapping_json: str = Form(...)
):
    environments = get_elasticsearch_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    try:
        mapping = json.loads(mapping_json)
        result = create_elasticsearch_index(env['host_url'], index_name, mapping, env.get('username'), env.get('password'))
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}



@app.post("/generate-mapping/elasticsearch/{env_id}")
async def generate_elasticsearch_custom_mapping(
        env_id: int,
        mapping_name: str = Form(...),
        mapping_fields: str = Form(...),
        analysis: Optional[str] = Form(None),
        similarities: Optional[str] = Form(None)
):
    environments = get_elasticsearch_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    # Parse mapping fields
    fields_data = json.loads(mapping_fields)
    mapping_fields_list = [MappingField(**field) for field in fields_data]

    analysis_settings = json.loads(analysis) if analysis else None
    similarity_settings = json.loads(similarities) if similarities else None

    # Generate mapping
    mapping = generate_elasticsearch_mapping(mapping_fields_list, analysis_settings, similarity_settings)

    # Save mapping
    index_mapping = IndexMapping(
        env_id=env_id,
        index_name="custom",
        mapping_name=mapping_name,
        mapping_json=json.dumps(mapping, indent=2)
    )
    mapping_id = save_mapping(index_mapping)

    return {
        "success": True,
        "mapping_id": mapping_id,
        "mapping": mapping,
        "source": "custom_fields"
    }

@app.get("/mappings")
async def list_mappings(env_id: Optional[int] = None):
    return get_mappings(env_id)

@app.get("/mappings/{mapping_id}")
async def get_mapping(mapping_id: int):
    mappings = get_mappings()
    mapping = next((m for m in mappings if m['id'] == mapping_id), None)
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return mapping

@app.delete("/mappings_v1/{mapping_id}")
async def delete_mapping_v1(mapping_id: int):
    with get_db() as conn:
        cursor = conn.cursor()

        # Check if mapping exists
        cursor.execute("SELECT id FROM mappings WHERE id = ?", (mapping_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Mapping not found")

        # Delete the mapping
        cursor.execute("DELETE FROM mappings WHERE id = ?", (mapping_id,))
        conn.commit()

        return {"success": True, "message": "Mapping deleted successfully"}

# Oracle Query Runner endpoints
@app.post("/oracle/query/{env_id}")
async def execute_oracle_query(env_id: int, query: str = Form(...)):
    """Execute SQL query on Oracle database"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Execute query
        import oracledb

        try:
            connection = oracledb.connect(
                user=env['username'],
                password=env['password'],
                dsn=env['url']
            )

            cursor = connection.cursor()
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch results
            results = cursor.fetchall()

            # Convert results to list of dictionaries
            data = []
            for row in results:
                row_dict = {}
                for i, value in enumerate(row):
                    if columns and i < len(columns):
                        # Handle different data types
                        if value is None:
                            row_dict[columns[i]] = None
                        elif isinstance(value, (int, float, str)):
                            row_dict[columns[i]] = value
                        else:
                            row_dict[columns[i]] = str(value)
                data.append(row_dict)

            cursor.close()
            connection.close()

            return JSONResponse({
                "success": True,
                "columns": columns,
                "data": data,
                "rowCount": len(data),
                "query": query
            })

        except Exception as e:
            return JSONResponse({
                "success": False,
                "error": str(e),
                "query": query
            })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


from fastapi import Form, HTTPException
from fastapi.responses import JSONResponse
import json
import sqlite3
import oracledb
from elasticsearch import Elasticsearch, helpers

def _json_load_maybe(v):
    if v is None:
        return None
    try:
        return json.loads(v)
    except Exception:
        return v

@app.post("/oracle/data-load")
async def oracle_data_load(
        oracle_env_id: int = Form(...),
        elastic_env_id: int = Form(...),
        index: str = Form(...),
        query: str = Form(...),
):
    """Execute Oracle query and load first 100 records into Elasticsearch."""
    try:
        oracle_envs = get_oracle_environments()
        oracle_env = next((e for e in oracle_envs if e["id"] == oracle_env_id), None)
        if not oracle_env:
            raise HTTPException(status_code=404, detail="Oracle environment not found")

        es_envs = get_elasticsearch_environments()
        es_env = next((e for e in es_envs if e["id"] == elastic_env_id), None)
        if not es_env:
            raise HTTPException(status_code=404, detail="Elasticsearch environment not found")

        # --- 1) Run the SELECT on Oracle and fetch up to 100 rows ---
        with oracledb.connect(
                user=oracle_env["username"],
                password=oracle_env["password"],
                dsn=oracle_env["url"],
        ) as connection:
            cursor = connection.cursor()
            cursor.execute(query)  # ensure this is a SELECT
            columns = [c[0] for c in cursor.description]
            rows = cursor.fetchmany(100)
            records = [dict(zip(columns, row)) for row in rows]
            print(records)

        # --- 2) Get the mapping from SQLite instead of Oracle ---
        db_path = "workflow_mappings.db"  # adjust to absolute path if needed
        with sqlite3.connect(db_path) as sconn:
            sconn.row_factory = sqlite3.Row
            scur = sconn.cursor()
            scur.execute(
                """
                SELECT id, mapping_name, index_name, environment_id,
                       tables, relationships, elasticsearch_mapping, table_structures,
                       total_fields, status, created_at, updated_at,
                       elasticsearch_created, error_message,oracle_query
                FROM workflow_mappings
                WHERE index_name = ? AND status = 'active'
                ORDER BY updated_at DESC
                    LIMIT 1
                """,
                (index,),
            )
            row = scur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Mapping not found in SQLite")

        mapping = {
            "id": row["id"],
            "mapping_name": row["mapping_name"],
            "index_name": row["index_name"],
            "environment_id": row["environment_id"],
            "tables": _json_load_maybe(row["tables"]),
            "relationships": _json_load_maybe(row["relationships"]),
            "elasticsearch_mapping": _json_load_maybe(row["elasticsearch_mapping"]),
            "table_structures": _json_load_maybe(row["table_structures"]),
            "total_fields": row["total_fields"],
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "elasticsearch_created": row["elasticsearch_created"],
            "error_message": row["error_message"],
        }

        # --- 3) Index to Elasticsearch using your mapper ---
        es_client = Elasticsearch(
            es_env["host_url"],
            basic_auth=(es_env.get("username"), es_env.get("password")) if es_env.get("username") else None,
            verify_certs=False,          # dev only; prefer a CA bundle in prod
            ssl_show_warn=False,         # hide SSL warnings if not verifying
            request_timeout=10
        )


        mapper = OracleElasticsearchMapper(es_client)

        all_column_names = extract_all_column_names(mapping["table_structures"])
        analysis_result = mapper.analyze_mapping(all_column_names, mapping["elasticsearch_mapping"])
        print("Mapping Analysis:")
        print(json.dumps(analysis_result, indent=2))
        print("\n" + mapper.get_mapping_report())

        result = mapper.bulk_index(records, index)
        print("\nBulk Index Result:")
        print(json.dumps(result, indent=2))

        return {
            "success": True,
            "indexed": len(records),
            "oracle_data": records,
            "elastic_data": result.get("converted_data", [])
        }

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

def extract_all_column_names(schema):
    """Extract all unique column names from the database schema"""
    all_columns = []

    for table_name, columns in schema.items():
        for column in columns:
            column_name = column["name"]
            if column_name not in all_columns:  # Avoid duplicates
                all_columns.append(column_name)

    return all_columns

def extract_columns_by_table(schema):
    """Extract column names organized by table"""
    tables = {}

    for table_name, columns in schema.items():
        tables[table_name] = [column["name"] for column in columns]

    return tables



@app.get("/oracle/query-tables/{env_id}")
async def get_oracle_tables_for_query(env_id: int):
    """Get list of tables for Oracle Query Runner"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        tables_result = get_oracle_tables(env['url'], env['username'], env['password'])

        # Extract tables array from nested structure
        if tables_result:
            return JSONResponse({"success": True, "tables": tables_result['tables']})
        else:
            return JSONResponse({"success": False, "error": tables_result.get('message', 'Unknown error'), "tables": []})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e), "tables": []})

# Oracle Mapping Builder endpoints
@app.get("/oracle/mapping-tables/{env_id}")
async def get_oracle_tables_for_mapping(env_id: int):
    """Get list of tables for Oracle Mapping Builder"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        tables_result = get_oracle_tables(env['url'], env['username'], env['password'])

        # Extract tables array from nested structure
        if tables_result:
            return JSONResponse({"success": True, "tables": tables_result['tables']})
        else:
            return JSONResponse({"success": False, "error": tables_result.get('message', 'Unknown error'), "tables": []})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e), "tables": []})

@app.get("/oracle/table-structure/{env_id}/{table_name}")
async def get_oracle_table_structure(env_id: int, table_name: str):
    """Get table structure for Oracle Mapping Builder"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        columns_result = get_table_columns(env['url'], env['username'], env['password'], table_name)

        # Extract columns array from nested structure if needed
        if isinstance(columns_result, dict) and 'columns' in columns_result:
            if columns_result.get('success'):
                return JSONResponse({"success": True, "columns": columns_result['columns'], "table_name": table_name})
            else:
                return JSONResponse({"success": False, "error": columns_result.get('message', 'Failed to load columns'), "columns": []})
        else:
            # Direct array response
            return JSONResponse({"success": True, "columns": columns_result, "table_name": table_name})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})



@app.post("/save-mapping-to-elasticsearch/{env_id}")
async def save_mapping_to_elasticsearch(
        env_id: int,
        index_name: str = Form(...),
        mapping_json: str = Form(...),
        mapping_name: str = Form(...)
):
    """Save generated mapping directly to Elasticsearch"""
    try:
        # Get Elasticsearch environment
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM elasticsearch_environments WHERE id = ?", (env_id,))
        env_row = cursor.fetchone()
        conn.close()

        if not env_row:
            return JSONResponse({"success": False, "error": "Environment not found"})

        env = {
            "id": env_row[0],
            "name": env_row[1],
            "host_url": env_row[2],
            "username": env_row[3],
            "password": env_row[4]
        }


        # Parse mapping JSON
        import json
        mapping = json.loads(mapping_json)
        print(mapping)

        # Create index in Elasticsearch
        result = create_elasticsearch_index(
            env['host_url'],
            index_name,
            mapping,
            env.get('username'),
            env.get('password')
        )

        if result.get('success'):
            # Also save to local database
            mapping_record = IndexMapping(
                env_id=env_id,
                index_name=index_name,
                mapping_name=mapping_name,
                mapping_json=mapping_json
            )

            mapping_id = save_mapping(mapping_record)

            return JSONResponse({
                "success": True,
                "message": f"Mapping saved successfully to Elasticsearch index '{index_name}'",
                "mapping_id": mapping_id,
                "elasticsearch_result": result
            })
        else:
            return JSONResponse({
                "success": False,
                "error": result.get('message', 'Failed to create index in Elasticsearch')
            })

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})

# Database operations
@contextmanager
def get_db():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def get_elasticsearch_environments():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM elasticsearch_environments ORDER BY name")
        return [dict(row) for row in cursor.fetchall()]

def get_oracle_environments():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM oracle_environments ORDER BY name")
        return [dict(row) for row in cursor.fetchall()]

def get_environments():
    """Get all environments (both Oracle and Elasticsearch)"""
    es_envs = get_elasticsearch_environments()
    oracle_envs = get_oracle_environments()

    # Add type identifier to each environment
    for env in es_envs:
        env['type'] = 'elasticsearch'
    for env in oracle_envs:
        env['type'] = 'oracle'

    return {'elasticsearch': es_envs, 'oracle': oracle_envs}

def save_environment(env):
    with get_db() as conn:
        cursor = conn.cursor()

        if isinstance(env, ElasticsearchEnvironment):
            if env.id:
                cursor.execute(
                    "UPDATE elasticsearch_environments SET name=?, host_url=?, username=?, password=? WHERE id=?",
                    (env.name, env.host_url, env.username, env.password, env.id)
                )
            else:
                cursor.execute(
                    "INSERT INTO elasticsearch_environments (name, host_url, username, password) VALUES (?, ?, ?, ?)",
                    (env.name, env.host_url, env.username, env.password)
                )
        elif isinstance(env, OracleEnvironment):
            if env.id:
                cursor.execute(
                    "UPDATE oracle_environments SET name=?, url=?, username=?, password=? WHERE id=?",
                    (env.name, env.url, env.username, env.password, env.id)
                )
            else:
                cursor.execute(
                    "INSERT INTO oracle_environments (name, url, username, password) VALUES (?, ?, ?, ?)",
                    (env.name, env.url, env.username, env.password)
                )

        conn.commit()
        return cursor.lastrowid

def delete_environment(env_id: int, env_type: str):
    with get_db() as conn:
        cursor = conn.cursor()

        if env_type == 'elasticsearch':
            cursor.execute("DELETE FROM elasticsearch_environments WHERE id=?", (env_id,))
            cursor.execute("DELETE FROM index_mappings WHERE env_id=?", (env_id,))
        elif env_type == 'oracle':
            cursor.execute("DELETE FROM oracle_environments WHERE id=?", (env_id,))
            # Also delete related mappings for Oracle environments
            cursor.execute("DELETE FROM index_mappings WHERE env_id=? AND env_id IN (SELECT id FROM oracle_environments)", (env_id,))

        conn.commit()

def save_mapping(mapping: IndexMapping):
    with get_db() as conn:
        cursor = conn.cursor()
        if mapping.id:
            cursor.execute(
                "UPDATE index_mappings SET env_id=?, index_name=?, mapping_name=?, mapping_json=? WHERE id=?",
                (mapping.env_id, mapping.index_name, mapping.mapping_name, mapping.mapping_json, mapping.id)
            )
        else:
            cursor.execute(
                "INSERT INTO index_mappings (env_id, index_name, mapping_name, mapping_json) VALUES (?, ?, ?, ?)",
                (mapping.env_id, mapping.index_name, mapping.mapping_name, mapping.mapping_json)
            )
        conn.commit()
        return cursor.lastrowid

def     get_mappings(env_id: Optional[int] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        if env_id:
            cursor.execute("""
                           SELECT m.*, e.name as env_name
                           FROM index_mappings m
                                    JOIN elasticsearch_environments e ON m.env_id = e.id
                           WHERE m.env_id = ?
                           ORDER BY m.created_at DESC
                           """, (env_id,))
        else:
            cursor.execute("""
                           SELECT m.*, e.name as env_name
                           FROM index_mappings m
                                    JOIN elasticsearch_environments e ON m.env_id = e.id
                           ORDER BY m.created_at DESC
                           """)
        return [dict(row) for row in cursor.fetchall()]

# Oracle database functions
def test_oracle_connection(url: str, username: str, password: str):
    """Test connection to Oracle database"""
    try:
        import oracledb

        # Parse connection string
        if '@' in url:
            connection_parts = url.split('@')
            dsn = connection_parts[1] if len(connection_parts) == 2 else url
        else:
            dsn = url

        with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            return {"success": True, "message": "Connection successful"}

    except ImportError:
        return {"success": False, "message": "oracledb module not installed"}
    except Exception as e:
        return {"success": False, "message": f"Connection failed: {str(e)}"}

def get_oracle_tables(url: str, username: str, password: str):
    """Get list of tables from Oracle database"""
    try:
        import oracledb

        if '@' in url:
            connection_parts = url.split('@')
            dsn = connection_parts[1] if len(connection_parts) == 2 else url
        else:
            dsn = url

        with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            table_names = [row[0] for row in cursor.fetchall()]
            # Convert to objects with table_name property for frontend compatibility
            tables = [{"table_name": name} for name in table_names]
            return { "tables": tables}

    except Exception as e:
        return {"success": False, "message": str(e), "tables": []}

def get_table_columns(url: str, username: str, password: str, table_name: str):
    """Get columns for a specific Oracle table"""
    try:
        import oracledb

        if '@' in url:
            connection_parts = url.split('@')
            dsn = connection_parts[1] if len(connection_parts) == 2 else url
        else:
            dsn = url

        with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
            cursor = connection.cursor()
            cursor.execute("""
                           SELECT column_name, data_type, data_length, nullable
                           FROM user_tab_columns
                           WHERE table_name = UPPER(:1)
                           ORDER BY column_id
                           """, (table_name,))

            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'length': row[2],
                    'nullable': row[3] == 'Y'
                })

            return {"success": True, "columns": columns}

    except Exception as e:
        return {"success": False, "message": str(e), "columns": []}

def oracle_to_elastic_type(oracle_type: str) -> str:
    """Convert Oracle data types to Elasticsearch field types"""
    type_mapping = {
        'VARCHAR2': 'text', 'CHAR': 'keyword', 'NUMBER': 'long',
        'DATE': 'date', 'TIMESTAMP': 'date', 'CLOB': 'text',
        'BLOB': 'binary', 'INTEGER': 'long', 'FLOAT': 'double'
    }
    return type_mapping.get(oracle_type.upper(), 'text')

# Elasticsearch connection functions
def test_elasticsearch_connection(host_url: str, username: Optional[str] = None, password: Optional[str] = None):
    """Test connection to Elasticsearch cluster"""
    try:
        # Clean up URL format
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        # Prepare auth
        auth = None
        if username and password:
            auth = (username, password)

        # Test connection with cluster health endpoint
        response = requests.get(
            f"{host_url}/_cluster/health",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            health_data = response.json()
            return {
                "success": True,
                "message": f"Connected successfully to {health_data.get('cluster_name', 'cluster')}",
                "cluster_name": health_data.get('cluster_name'),
                "status": health_data.get('status'),
                "number_of_nodes": health_data.get('number_of_nodes')
            }
        else:
            return {
                "success": False,
                "message": f"Connection failed with status {response.status_code}: {response.text}"
            }

    except requests.exceptions.ConnectionError:
        return {"success": False, "message": "Could not connect to Elasticsearch. Check URL and network connectivity."}
    except requests.exceptions.Timeout:
        return {"success": False, "message": "Connection timeout. Elasticsearch may be unreachable."}
    except Exception as e:
        return {"success": False, "message": f"Connection error: {str(e)}"}

def get_elasticsearch_indices(host_url: str, username: Optional[str] = None, password: Optional[str] = None):
    """Get all indices from Elasticsearch cluster"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        response = requests.get(
            f"{host_url}/_cat/indices?format=json&h=index,docs.count,store.size",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch indices: {response.status_code}")

    except Exception as e:
        raise Exception(f"Error fetching indices: {str(e)}")

def get_elasticsearch_mapping(host_url: str, index_name: str, username: Optional[str] = None, password: Optional[str] = None):
    """Get mapping for a specific index"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        response = requests.get(
            f"{host_url}/{index_name}/_mapping",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch mapping: {response.status_code}")

    except Exception as e:
        raise Exception(f"Error fetching mapping: {str(e)}")



# Elasticsearch mapping generation helpers
def get_default_field_mapping(field_type: str) -> dict:
    """Get default mapping configuration for field types"""
    default_mappings = {
        'text': {
            'type': 'text',
            'fields': {
                'keyword': {
                    'type': 'keyword',
                    'ignore_above': 256
                }
            }
        },
        'keyword': {'type': 'keyword'},
        'long': {'type': 'long'},
        'integer': {'type': 'integer'},
        'double': {'type': 'double'},
        'float': {'type': 'float'},
        'date': {'type': 'date'},
        'boolean': {'type': 'boolean'},
        'binary': {'type': 'binary'},
        'nested': {'type': 'nested'},
        'object': {'type': 'object'},
        'join': {'type': 'join'},
        'dense_vector': {'type': 'dense_vector'},
        'sparse_vector': {'type': 'sparse_vector'}
    }
    return default_mappings.get(field_type, {'type': 'text'})

def generate_elasticsearch_mapping(
    mapping_fields: List[MappingField],
    analysis: Optional[Dict[str, Any]] = None,
    similarities: Optional[Dict[str, Any]] = None
) -> dict:
    """Generate Elasticsearch mapping from user-defined fields"""
    # First build field definitions
    field_defs: Dict[str, Any] = {}
    for field in mapping_fields:
        field_def = get_default_field_mapping(field.elastic_type)

        # Apply custom properties
        if isinstance(field.properties, dict):
            field_def.update(field.properties)

        # Handle nested/object field children
        if field.nested_fields and field.field_type in ['nested', 'object']:
            field_def['properties'] = {}
            for nested_field in field.nested_fields:
                nested_def = get_default_field_mapping(nested_field.get('type', 'text'))
                if nested_field.get('properties'):
                    nested_def.update(nested_field['properties'])
                field_def['properties'][nested_field['name']] = nested_def

        field_defs[field.field_name] = field_def

    # Assemble hierarchy using parent_field references
    root_properties: Dict[str, Any] = {}
    for field in mapping_fields:
        field_def = field_defs[field.field_name]
        if field.parent_field and field.parent_field in field_defs:
            parent_def = field_defs[field.parent_field]
            parent_def.setdefault('properties', {})[field.field_name] = field_def
        else:
            root_properties[field.field_name] = field_def

    mapping: Dict[str, Any] = {"mappings": {"properties": root_properties}}
    if analysis or similarities:
        mapping["settings"] = {}
        if analysis:
            mapping["settings"]["analysis"] = analysis
        if similarities:
            mapping["settings"]["similarity"] = similarities
    return mapping

# API Routes
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    environments = get_environments()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "environments": environments
    })

@app.post("/environments/elasticsearch")
async def create_elasticsearch_environment(
        name: str = Form(...),
        host_url: str = Form(...),
        username: str = Form(None),
        password: str = Form(None)
):
    env = ElasticsearchEnvironment(name=name, host_url=host_url, username=username, password=password)
    env_id = save_environment(env)
    return JSONResponse({"success": True, "id": env_id, "type": "elasticsearch"})

@app.post("/environments/oracle")
async def create_oracle_environment(
        name: str = Form(...),
        url: str = Form(...),
        username: str = Form(...),
        password: str = Form(...)
):
    env = OracleEnvironment(name=name, url=url, username=username, password=password)
    env_id = save_environment(env)
    return JSONResponse({"success": True, "id": env_id, "type": "oracle"})

@app.get("/environments")
async def list_environments():
    return get_environments()

@app.delete("/environments/{env_type}/{env_id}")
async def remove_environment(env_id: int, env_type: str):
    delete_environment(env_id, env_type)
    return JSONResponse({"success": True})

@app.post("/test-connection/{env_type}/{env_id}")
async def test_connection(env_id: int, env_type: str):
    if env_type == 'elasticsearch':
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        result = test_elasticsearch_connection(env['host_url'], env.get('username'), env.get('password'))
        return result

    elif env_type == 'oracle':
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        result = test_oracle_connection(env['url'], env['username'], env['password'])
        return result

    else:
        raise HTTPException(status_code=400, detail="Invalid environment type")

@app.get("/indices/{env_id}")
async def list_indices(env_id: int):
    environments = get_elasticsearch_environments()
    target_id = int(env_id)
    env = next((item for item in environments if item['id'] == target_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    indices = get_elasticsearch_indices(env['host_url'], env.get('username'), env.get('password'))
    return indices

@app.get("/tables/{env_id}")
async def list_tables(env_id: str):
    print(env_id)
    environments = get_oracle_environments()
    print(environments)
    print(env_id)
    target_id = int(env_id)
    env = next((item for item in environments if item['id'] == target_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    tables = get_oracle_tables(env['url'], env['username'], env['password'])
    print(tables)
    return tables

from typing import Optional, Dict, Any
from pydantic import BaseModel

class MappingField(BaseModel):
    field_name: str
    oracle_type: str
    elastic_type: str
    field_type: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None
    nested_fields: Optional[List[Dict[str, Any]]] = []

@app.get("/columns/{env_id}/{table_name}")
async def list_columns(env_id: int, table_name: str):
    environments = get_oracle_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    columns = get_table_columns(env['url'], env['username'], env['password'], table_name)
    return columns

@app.get("/mapping/{env_id}/{index_name}")
async def get_index_mapping(env_id: int, index_name: str):
    environments = get_environments()
    print(environments)
    print(env_id)
    print(index_name)
    env = next((item for item in environments['elasticsearch'] if item['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")
    try:
        # Log the request details for debugging
        print(f"DEBUG: Fetching mapping for index '{index_name}' from environment ID {env_id}")
        print(f"DEBUG: Elasticsearch URL: {env['host_url']}")

        mapping = get_elasticsearch_mapping(env['host_url'], index_name, env.get('username'), env.get('password'))
        print(f"DEBUG: Successfully retrieved mapping for '{index_name}'")
        print(mapping)
        return mapping
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving mapping: {str(e)}")
        error_msg = str(e)
        print(f"ERROR: Failed to retrieve mapping for '{index_name}': {error_msg}")


@app.post("/create-index/{env_id}")
async def create_index(
        env_id: int,
        index_name: str = Form(...),
        mapping_json: str = Form(...)
):
    environments = get_environments()
    environments = get_elasticsearch_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    try:
        mapping = json.loads(mapping_json)
        result = create_elasticsearch_index(env['host_url'], index_name, mapping, env.get('username'), env.get('password'))
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/generate-mapping-v1/oracle/{env_id}")
async def generate_oracle_mapping_v1(
        env_id: int,
        table_name: str = Form(...),
        mapping_name: str = Form(...),
        mapping_fields: str = Form(...)
):
    environments = get_oracle_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    # Parse mapping fields
    fields_data = json.loads(mapping_fields)
    mapping_fields_list = [MappingField(**field) for field in fields_data]


    # Generate mapping
    mapping = generate_elasticsearch_mapping(mapping_fields_list)

    # Save mapping
    index_mapping = IndexMapping(
        env_id=env_id,
        index_name=table_name,
        mapping_name=mapping_name,
        mapping_json=json.dumps(mapping, indent=2)
    )
    mapping_id = save_mapping(index_mapping)

    return {
        "success": True,
        "mapping_id": mapping_id,
        "mapping": mapping,
        "source": "oracle_table",
        "table_name": table_name
    }

@app.post("/generate-mapping/oracle/{env_id}")
async def generate_oracle_mapping(
        env_id: int,
        table_name: str = Form(...),
        mapping_name: str = Form(...),
        mapping_fields: str = Form(...)
):
    # Simulate environment lookup (replace with your actual logic)
    environments = [{'id': env_id}]  # dummy list with one env
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    # Parse mapping_fields JSON string to Python list
    try:
        fields_data = json.loads(mapping_fields)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="mapping_fields must be valid JSON")

    # Validate each field dict against MappingField model
    try:
        # Try the first approach
        mapping_fields_list = [MappingField(**field) for field in fields_data]
        print("✅ First approach worked!")
        for field in mapping_fields_list:
            print(f"  {field.field_name}: oracle_type={field.oracle_type}")
    except Exception as e:
        print(f"❌ First approach failed: {e}")

    # Try the flexible approach
    try:
        mapping_fields_list = [MappingFieldFlexible(**field) for field in fields_data]
        print("✅ Flexible approach worked!")
        for field in mapping_fields_list:
            print(f"  {field.field_name}: oracle_type={field.oracle_type}")
    except Exception as e:
        print(f"❌ Both approaches failed: {e}")

    # Generate mapping
    print(mapping_fields_list)
    mapping = generate_elasticsearch_mapping(mapping_fields_list)
    # Save mapping
    print(mapping)
    index_mapping = IndexMapping(
        env_id=env_id,
        index_name=table_name,
        mapping_name=mapping_name,
        mapping_json=json.dumps(mapping, indent=2)
    )
    mapping_id = save_mapping(index_mapping)

    return {
        "success": True,
        "mapping_id": mapping_id,
        "mapping": mapping,
        "source": "oracle_table",
        "table_name": table_name
    }


@app.post("/generate-mapping/elasticsearch/{env_id}")
async def generate_elasticsearch_custom_mapping(
        env_id: int,
        mapping_name: str = Form(...),
        mapping_fields: str = Form(...),
        analysis: Optional[str] = Form(None),
        similarities: Optional[str] = Form(None)
):
    environments = get_elasticsearch_environments()
    env = next((e for e in environments if e['id'] == env_id), None)
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    # Parse mapping fields
    fields_data = json.loads(mapping_fields)
    mapping_fields_list = [MappingField(**field) for field in fields_data]

    # Generate mapping
    analysis_settings = json.loads(analysis) if analysis else None
    similarity_settings = json.loads(similarities) if similarities else None
    mapping = generate_elasticsearch_mapping(mapping_fields_list, analysis_settings, similarity_settings)

    # Save mapping
    index_mapping = IndexMapping(
        env_id=env_id,
        index_name="custom",
        mapping_name=mapping_name,
        mapping_json=json.dumps(mapping, indent=2)
    )
    mapping_id = save_mapping(index_mapping)

    return {
        "success": True,
        "mapping_id": mapping_id,
        "mapping": mapping,
        "source": "custom_fields"
    }

@app.get("/mappings")
async def list_mappings(env_id: Optional[int] = None):
    return get_mappings(env_id)

@app.get("/mappings/{mapping_id}")
async def get_mapping(mapping_id: int):
    mappings = get_mappings()
    mapping = next((m for m in mappings if m['id'] == mapping_id), None)
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return mapping

@app.delete("/mappings/{mapping_id}")
async def delete_mapping(mapping_id: int):
    with get_db() as conn:
        cursor = conn.cursor()

        # Check if mapping exists
        cursor.execute("SELECT id FROM mappings WHERE id = ?", (mapping_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Mapping not found")

        # Delete the mapping
        cursor.execute("DELETE FROM mappings WHERE id = ?", (mapping_id,))
        conn.commit()

        return {"success": True, "message": "Mapping deleted successfully"}

# Oracle Query Runner endpoints
@app.post("/oracle/query/{env_id}")
async def execute_oracle_query(env_id: int, query: str = Form(...)):
    """Execute SQL query on Oracle database"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Execute query
        import oracledb

        try:
            connection = oracledb.connect(
                user=env['username'],
                password=env['password'],
                dsn=env['url']
            )

            cursor = connection.cursor()
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch results
            results = cursor.fetchall()

            # Convert results to list of dictionaries
            data = []
            for row in results:
                row_dict = {}
                for i, value in enumerate(row):
                    if columns and i < len(columns):
                        # Handle different data types
                        if value is None:
                            row_dict[columns[i]] = None
                        elif isinstance(value, (int, float, str)):
                            row_dict[columns[i]] = value
                        else:
                            row_dict[columns[i]] = str(value)
                data.append(row_dict)

            cursor.close()
            connection.close()

            return JSONResponse({
                "success": True,
                "columns": columns,
                "data": data,
                "rowCount": len(data),
                "query": query
            })

        except Exception as e:
            return JSONResponse({
                "success": False,
                "error": str(e),
                "query": query
            })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/oracle/query-tables/{env_id}")
async def get_oracle_tables_for_query(env_id: int):
    """Get list of tables for Oracle Query Runner"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        tables_result = get_oracle_tables(env['url'], env['username'], env['password'])

        # Extract tables array from nested structure
        if tables_result:
            return JSONResponse({"success": True, "tables": tables_result['tables']})
        else:
            return JSONResponse({"success": False, "error": tables_result.get('message', 'Unknown error'), "tables": []})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e), "tables": []})

# Oracle Mapping Builder endpoints
@app.get("/oracle/mapping-tables/{env_id}")
async def get_oracle_tables_for_mapping(env_id: int):
    """Get list of tables for Oracle Mapping Builder"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        tables_result = get_oracle_tables(env['url'], env['username'], env['password'])

        # Extract tables array from nested structure
        if tables_result.get('success'):
            return JSONResponse({"success": True, "tables": tables_result['tables']})
        else:
            return JSONResponse({"success": False, "error": tables_result.get('message', 'Unknown error'), "tables": []})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e), "tables": []})

@app.get("/oracle/table-structure/{env_id}/{table_name}")
async def get_oracle_table_structure(env_id: int, table_name: str):
    """Get table structure for Oracle Mapping Builder"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        columns_result = get_table_columns(env['url'], env['username'], env['password'], table_name)

        # Extract columns array from nested structure if needed
        if isinstance(columns_result, dict) and 'columns' in columns_result:
            if columns_result.get('success'):
                return JSONResponse({"success": True, "columns": columns_result['columns'], "table_name": table_name})
            else:
                return JSONResponse({"success": False, "error": columns_result.get('message', 'Failed to load columns'), "columns": []})
        else:
            # Direct array response
            return JSONResponse({"success": True, "columns": columns_result, "table_name": table_name})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})

@app.post("/oracle/generate-table-mapping/{env_id}")
async def generate_oracle_table_mapping(
        env_id: int,
        table_name: str = Form(...),
        mapping_name: str = Form(...),
        selected_columns: str = Form(...)
):
    """Generate Elasticsearch mapping from Oracle table structure"""
    try:
        environments = get_oracle_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Parse selected columns
        import json
        columns_list = json.loads(selected_columns)

        # Get table columns
        all_columns_result = get_table_columns(env['url'], env['username'], env['password'], table_name)

        # Handle nested structure response
        if isinstance(all_columns_result, dict) and 'columns' in all_columns_result:
            all_columns = all_columns_result['columns'] if all_columns_result.get('success') else []
        else:
            all_columns = all_columns_result if isinstance(all_columns_result, list) else []

        # Filter selected columns - handle both 'name' and 'column_name' properties
        selected_column_data = []
        for col in all_columns:
            col_name = col.get('name') or col.get('column_name', '')
            if col_name in columns_list:
                selected_column_data.append(col)

        # Generate mapping
        mapping_fields = []
        for col in selected_column_data:
            col_name = col.get('name') or col.get('column_name', 'unknown')
            col_type = col.get('type') or col.get('data_type', 'VARCHAR2')
            elastic_type = oracle_to_elastic_type(col_type)
            field = MappingFieldFlexible(
                field_name=col_name.lower(),
                field_type=elastic_type,
                properties={}
            )
            mapping_fields.append(field)

        # Generate Elasticsearch mapping
        mapping = generate_elasticsearch_mapping(mapping_fields)

        # Save mapping
        mapping_record = IndexMapping(
            env_id=env_id,
            index_name=table_name.lower(),
            mapping_name=mapping_name,
            mapping_json=json.dumps(mapping, indent=2)
        )

        mapping_id = save_mapping(mapping_record)

        return JSONResponse({
            "success": True,
            "mapping": mapping,
            "mapping_id": mapping_id,
            "table_name": table_name,
            "mapping_name": mapping_name
        })

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})

@app.post("/save-mapping-to-elasticsearch_v1/{env_id}")
async def save_mapping_to_elasticsearch_v1(
        env_id: int,
        index_name: str = Form(...),
        mapping_json: str = Form(...),
        mapping_name: str = Form(...)
):
    try:
        # Get ES environment from DB
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM elasticsearch_environments WHERE id = ?", (env_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return JSONResponse({"success": False, "error": "Environment not found"})

        env = {
            "id": row[0],
            "name": row[1],
            "host_url": row[2],
            "username": row[3],
            "password": row[4]
        }

        # Parse mapping JSON
        mapping = json.loads(mapping_json)
        print('2'+mapping)
        # Call helper function to create index
        result = create_elasticsearch_index(env['host_url'], index_name, mapping, env['username'], env['password'])

        if result.get('success'):
            # Save to local DB
            mapping_record = IndexMapping(
                env_id=env_id,
                index_name=index_name,
                mapping_name=mapping_name,
                mapping_json=mapping_json
            )
            mapping_id = save_mapping(mapping_record)

            return {
                "success": True,
                "mapping_id": mapping_id,
                "elasticsearch_result": result,
                "message": f"Mapping saved to index '{index_name}'"
            }

        return {"success": False, "error": result.get('message', 'Failed to create index')}

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})


# ================================
# UI Field Mapping Form Builder API Endpoints
# ================================

@app.post("/save-form")
async def save_form(request: Request):
    """Save form configuration"""
    try:
        form_data = await request.json()

        # Validate required fields
        if not form_data.get('name') or not form_data.get('url'):
            return JSONResponse({
                "success": False,
                "error": "Form name and URL are required"
            })

        # Check if URL already exists
        existing_form = get_form_configuration_by_url(form_data['url'])
        if existing_form:
            return JSONResponse({
                "success": False,
                "error": "Form URL already exists. Please choose a different URL."
            })

        # Create form configuration
        form_config = FormConfiguration(
            name=form_data['name'],
            url=form_data['url'],
            environment=form_data['environment'],
            index=form_data['index'],
            fields=form_data['fields']
        )

        form_id = save_form_configuration(form_config)

        return JSONResponse({
            "success": True,
            "message": "Form configuration saved successfully",
            "form_id": form_id
        })

    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to save form configuration: {str(e)}"
        })

@app.get("/saved-forms")
async def get_saved_forms():
    """Get all saved form configurations"""
    try:
        db_path = os.path.abspath('database.db')
        print(db_path)
        forms = get_form_configurations()
        return JSONResponse({
            "success": True,
            "forms": forms
        })
    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to load saved forms: {str(e)}"
        })

@app.delete("/delete-form/{form_id}")
async def delete_form(form_id: int):
    """Delete a form configuration"""
    try:
        success = delete_form_configuration(form_id)

        if success:
            return JSONResponse({
                "success": True,
                "message": "Form deleted successfully"
            })
        else:
            return JSONResponse({
                "success": False,
                "error": "Form not found"
            })

    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to delete form: {str(e)}"
        })




@app.get("/form/{form_url}")
async def render_form(form_url: str, request: Request):
    """Render the dynamic form by URL"""
    try:

        form_config = get_form_configuration_by_url(form_url)

        if not form_config:
            raise HTTPException(status_code=404, detail="Form not found")

        # Parse fields JSON if it's a string
        if isinstance(form_config.get('fields_json'), str):
            form_config['fields'] = json.loads(form_config['fields_json'])
        elif isinstance(form_config.get('fields'), str):
            form_config['fields'] = json.loads(form_config['fields'])

        # Make sure environment ID is available
        if 'environment' not in form_config:
            form_config['environment'] = form_config.get('environment_id', 4)  # Default fallback

        return templates.TemplateResponse("form.html", {
            "request": request,
            "form_config": form_config
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to render form: {str(e)}")

@app.post("/submit-form_v1/{form_url}")
async def submit_form_v1(form_url: str, request: Request):
    """Handle form submission and query Elasticsearch"""
    try:
        form_config = get_form_configuration_by_url(form_url)

        if not form_config:
            return JSONResponse({
                "success": False,
                "error": "Form not found"
            })

        # Get form submission data
        form_data = await request.json()

        # Build Elasticsearch query based on form fields
        query_body = {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }

        # Process form fields and build query
        for field_name, field_config in form_config['fields'].items():
            if field_name in form_data and form_data[field_name]:
                value = form_data[field_name]

                # Build query based on field type and role
                if field_config.get('role') == 'key':
                    # Exact match for key fields
                    query_body['query']['bool']['must'].append({
                        "term": {field_name: value}
                    })
                else:
                    # Text search for value fields
                    if field_config.get('inputType') == 'text':
                        query_body['query']['bool']['must'].append({
                            "match": {field_name: value}
                        })
                    else:
                        query_body['query']['bool']['must'].append({
                            "term": {field_name: value}
                        })

        # If no filters, return all documents
        if not query_body['query']['bool']['must']:
            query_body = {"query": {"match_all": {}}}

        # Execute Elasticsearch query
        es_url = f"http://{form_config['host_url']}/{form_config['index_name']}/_search"

        auth = None
        if form_config.get('username') and form_config.get('password'):
            auth = (form_config['username'], form_config['password'])

        response = requests.post(es_url, json=query_body, auth=auth)

        if response.status_code == 200:
            result = response.json()

            # Format results for display
            hits = result.get('hits', {}).get('hits', [])
            formatted_results = []

            for hit in hits:
                source = hit.get('_source', {})
                formatted_results.append({
                    "id": hit.get('_id'),
                    "score": hit.get('_score'),
                    "data": source
                })

            return JSONResponse({
                "success": True,
                "results": formatted_results,
                "total": result.get('hits', {}).get('total', {}).get('value', 0),
                "query": query_body
            })
        else:
            return JSONResponse({
                "success": False,
                "error": f"Elasticsearch query failed: {response.text}"
            })

    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to submit form: {str(e)}"
        })


# INSERT AFTER LINE 1000: Enhanced Form Configuration Endpoints

@app.post("/save-enhanced-form")
async def save_enhanced_form_configuration(request: Request):
    """Save enhanced form configuration with field mappings"""
    try:
        form_data = await request.json()

        # Validate enhanced form data
        required_fields = ['name', 'url', 'environment', 'index', 'fields']
        for field in required_fields:
            if field not in form_data:
                return JSONResponse({
                    "success": False,
                    "error": f"Missing required field: {field}"
                })

        # Check if URL already exists
        existing_form = get_form_configuration_by_url(form_data['url'])
        if existing_form:
            return JSONResponse({
                "success": False,
                "error": "Form URL already exists. Please choose a different URL."
            })

        # Create enhanced form configuration
        enhanced_config = FormConfiguration(
            name=form_data['name'],
            url=form_data['url'],
            environment=form_data['environment'],
            index=form_data['index'],
            fields=form_data['fields']  # This now includes enhanced field configs
        )

        form_id = save_form_configuration(enhanced_config)

        return JSONResponse({
            "success": True,
            "message": "Enhanced form configuration saved successfully",
            "form_id": form_id,
            "form_url": f"/form/{form_data['url']}"
        })

    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to save enhanced form configuration: {str(e)}"
        })

@app.get("/enhanced-form-config/{form_url}")
async def get_enhanced_form_configuration(form_url: str):
    """Get enhanced form configuration with all field mappings"""
    try:
        form_config = get_form_configuration_by_url(form_url)

        if not form_config:
            raise HTTPException(status_code=404, detail="Enhanced form not found")

        # Parse enhanced fields configuration
        enhanced_fields = json.loads(form_config['fields_json'])

        # Add enhanced field processing
        for field_name, field_config in enhanced_fields.items():
            if field_config.get('inputType') == 'checkbox' and field_config.get('sourceIndex'):
                # Load actual values from source index if needed
                try:
                    if field_config.get('selectedValues'):
                        field_config['valuesCount'] = len(field_config['selectedValues'])
                    else:
                        field_config['valuesCount'] = 0
                except:
                    field_config['valuesCount'] = 0

        form_config['enhanced_fields'] = enhanced_fields

        return JSONResponse({
            "success": True,
            "form_config": form_config
        })

    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to load enhanced form configuration: {str(e)}"
        })

def build_terms_aggregation(field_name: str,field_type: str) -> dict:
    if '.' in field_name:
        # Consider as nested
        nested_path = field_name.split('.')[0]  # 'address.city' -> 'address'

        field_name = field_name + ".keyword" if field_type != "keyword" else field_name
        return {
            "size": 0,
            "aggs": {
                "nested_agg": {
                    "nested": {
                        "path": nested_path
                    },
                    "aggs": {
                        "unique_values": {
                            "terms": {
                                "field": field_name,
                                "size": 1000
                            }
                        }
                    }
                }
            }
        }
    else:
        field_name = field_name + ".keyword" if field_type != "keyword" else field_name
        return {
            "size": 0,
            "aggs": {
                "unique_values": {
                    "terms": {
                        "field": field_name,
                        "size": 1000
                    }
                }
            }
        }


@app.get("/field-values/{env_id}/{index_name}/{field_name}")
async def get_field_values_for_dropdown_v1(env_id: int, index_name: str, field_name: str):
    """Get unique values for a field from an index for dropdown/checkbox population"""
    try:
        # Get Elasticsearch environment
        print(field_name)
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Build Elasticsearch aggregation query to get unique values



        if not env['host_url'].startswith(('http://', 'https://')):
            env['host_url'] = f"http://{env['host_url']}"

        auth = None
        if env.get('username') and env.get('password'):
            auth = (env['username'], env['password'])


        # NEW: Get field mapping to check if it's boolean
        def get_field_type():
            try:
                mapping_response = requests.get(
                    f"{env['host_url']}/{index_name}/_mapping",
                    auth=auth,
                    timeout=10,
                    verify=False
                )

                if mapping_response.status_code == 200:
                    mapping = mapping_response.json()
                    properties = mapping.get(index_name, {}).get('mappings', {}).get('properties', {})

                    if '.' in field_name:
                        # Handle nested fields
                        field_parts = field_name.split('.')
                        current_properties = properties

                        for part in field_parts:
                            if part in current_properties:
                                field_info = current_properties[part]
                                if field_info.get('type') == 'nested' and 'properties' in field_info:
                                    current_properties = field_info['properties']
                                elif 'properties' in field_info:
                                    current_properties = field_info['properties']
                                else:
                                    return field_info.get('type')
                            else:
                                return None
                        return None
                    else:
                        # Handle regular fields
                        return properties.get(field_name, {}).get('type')
                return None
            except Exception as e:
                print(f"Error getting field mapping: {e}")
                return None

        field_type = get_field_type()
        is_boolean_field = (field_type == 'boolean')
        print(f"Field type: {field_type}, is_boolean: {is_boolean_field}")


        query = build_terms_aggregation(field_name,field_type)
        print(query)


        response = requests.get(
            f"{env['host_url']}/{index_name}/_search",
            json= query,
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            result = response.json()
            if '.' in field_name:
                buckets = result.get('aggregations', {}) \
                    .get('nested_agg', {}) \
                    .get('unique_values', {}) \
                    .get('buckets', [])
            else:
                buckets = result.get('aggregations', {}) \
                    .get('unique_values', {}) \
                    .get('buckets', [])

            values = []
            for bucket in buckets:
                bucket_value = bucket['key']

                # NEW: Convert boolean values
                if is_boolean_field:
                    if bucket_value == 1:
                        bucket_value = True
                    elif bucket_value == 0:
                        bucket_value = False

                values.append({
                    'value': bucket_value
                })

            print(values)


            return JSONResponse({
                "success": True,
                "values": values,
                "total_count": len(values)
            })
        else:
            raise Exception(f"Elasticsearch query failed: {response.status_code}")


    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to get field values: {str(e)}",
            "values": []
        })
# Initialize database on startup



@app.post("/submit-form-with-logic/{form_url}")
async def submit_form_with_logic(form_url: str, request: Request):
    """Handle form submission with logical operators between fields"""
    try:
        form_config = get_form_configuration_by_url(form_url)
        if not form_config:
            return JSONResponse({"success": False, "error": "Form not found"})

        # Get form submission data
        form_data = await request.json()
        logical_structure = form_data.get('logicalStructure', [])
        field_values = form_data.get('fieldValues', {})

        # Build complex Elasticsearch query with logical operators
        query_body = build_logical_query(logical_structure, field_values, form_config)

        # Execute query
        es_url = f"http://{form_config['host_url']}/{form_config['index_name']}/_search"
        auth = None
        if form_config.get('username') and form_config.get('password'):
            auth = (form_config['username'], form_config['password'])

        response = requests.post(es_url, json=query_body, auth=auth)

        if response.status_code == 200:
            result = response.json()
            hits = result.get('hits', {}).get('hits', [])

            formatted_results = []
            for hit in hits:
                formatted_results.append({
                    "id": hit.get('_id'),
                    "score": hit.get('_score'),
                    "data": hit.get('_source', {})
                })

            return JSONResponse({
                "success": True,
                "results": formatted_results,
                "total": result.get('hits', {}).get('total', {}).get('value', 0),
                "query": query_body,
                "logical_structure": logical_structure
            })
        else:
            return JSONResponse({
                "success": False,
                "error": f"Elasticsearch query failed: {response.text}"
            })

    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": f"Failed to submit form: {str(e)}"
        })

def build_logical_query(logical_structure, field_values, form_config):
    """Build Elasticsearch query with logical operators"""
    if not logical_structure:
        return {"query": {"match_all": {}}}

    def process_structure(structure):
        query_parts = []
        current_operator = "must"  # Default to AND

        for item in structure:
            if item['type'] == 'field':
                field_name = item['config']['name']
                field_value = field_values.get(field_name)

                if field_value:
                    field_query = create_field_query(item['config'], field_value)
                    query_parts.append(field_query)

            elif item['type'] == 'operator':
                current_operator = {
                    'AND': 'must',
                    'OR': 'should',
                    'NOT': 'must_not'
                }.get(item['operator'], 'must')

        # Build bool query based on operator
        if not query_parts:
            return {"match_all": {}}

        if len(query_parts) == 1:
            return query_parts[0]

        bool_query = {"bool": {}}

        if current_operator == 'should':
            bool_query["bool"]["should"] = query_parts
            bool_query["bool"]["minimum_should_match"] = 1
        else:
            bool_query["bool"][current_operator] = query_parts

        return bool_query

    main_query = process_structure(logical_structure)

    return {
        "query": main_query,
        "size": 100,
        "sort": [{"_score": {"order": "desc"}}]
    }

def create_field_query(field_config, field_value):
    """Create appropriate Elasticsearch query for field"""
    field_name = field_config['name']

    if field_config.get('role') == 'key':
        # Exact match for key fields
        return {"term": {field_name: field_value}}
    else:
        # Text search for value fields
        if field_config.get('inputType') == 'text':
            return {"match": {field_name: field_value}}
        else:
            return {"term": {field_name: field_value}}

class FormSubmissionData(BaseModel):
    fields: Dict[str, Any]
    metadata: Dict[str, Any]

class MultiValueField(BaseModel):
    type: str = "multi_value"
    operator: str = "AND"  # AND, OR, NOT
    values: List[str]
    sourceConfig: Optional[Dict[str, str]] = None


@app.post("/submit-form/{form_url}")
async def submit_enhanced_form(form_url: str, submission_data: FormSubmissionData):
    """
    Enhanced form submission handler that captures all parameter types
    """
    try:
        # Get form configuration
        form_config = get_form_configuration_by_url(form_url)
        if not form_config:
            raise HTTPException(status_code=404, detail="Form not found")

        # Parse enhanced fields configuration
        if isinstance(form_config.get('fields_json'), str):
            form_config['fields'] = json.loads(form_config['fields_json'])

        # Build comprehensive Elasticsearch query
        query_body = build_enhanced_elasticsearch_query(
            submission_data.fields,
            form_config,
            submission_data.metadata
        )

        # Execute query
        print(query_body)
        result = await execute_elasticsearch_query(form_config, query_body)
        request_obj = ElasticsearchQueryRequest(
            query=query_body,
            index_name=form_config.get("index_name"),
            context=""
        )
        generated_questions=None
        try:
            generated_questions= await convert_query_to_questions(request_obj)
        except Exception as e:
            print(generated_questions)

        # Log submission for analytics
        log_form_submission(form_url, submission_data, result)

        return {
            "success": True,
            "results": result.get("hits", []),
            "total": result.get("total", 0),
            "took": result.get("took", 0),
            "query": query_body,
            "metadata": {
                "form_url": form_url,
                "field_count": len(submission_data.fields),
                "timestamp": submission_data.metadata.get("timestamp")
            },
            "generated_questions": generated_questions.generated_questions if generated_questions is not None else None
        }

    except Exception as e:
        print(f"Form submission error: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "details": f"Failed to process form submission for {form_url}"
        }

def build_enhanced_elasticsearch_query(fields: Dict[str, Any], form_config: Dict, metadata: Dict) -> Dict:
    """
    Build comprehensive Elasticsearch query from form fields with enhanced logic
    """
    print(fields)
    print(form_config.get("index_name"))
    #temp=build_query_v1(fields,"AND",form_config.get("index_name"))
    #temp2=transform_query_v1(temp)
    #print(temp2)
    #query=build_es_query_v2(temp2)

    try:
        root_field_list, inner_field_list, _, nested_field_list, _ ,parent_child_list= fetch_field_lists(form_config.get("environment"), form_config.get("index_name"))
        print(parent_child_list)
        print(inner_field_list)
        print(fields)
        print("......")
        if inner_field_list is not None and len(inner_field_list) > 0:
            print("*******************************************************###########")
            fieldsv1=add_prefix_to_keys(fields,inner_field_list,parent_child_list)
            print(fieldsv1)
            tempv3=build_query_v6(fieldsv1,"AND",form_config.get("index_name"),nested_field_list,inner_field_list)
            print(tempv3)
            print("......")
            tempv5=transform_query_v6(tempv3)
            print(tempv5)
            print("......")
            queryv3=build_es_query_v3(tempv5)
            print(queryv3)
            print("*******************************************************###########")
            return queryv3
        else:
            print("*******************************************************")
            tempv1=build_query_v6(fields,"AND",form_config.get("index_name"))
            print(tempv1)
            tempv2=transform_query_v6(tempv1)
            print(tempv2)
            queryv2=build_es_query_v3(tempv2)
            print(queryv2)
            print("*******************************************************")
            return queryv2
    except Exception as e:
        print(e)





def build_query_v1(result: dict, operator: str,index: str):

    main_groups = []
    base_conditions = []

    for key, value in result.items():
        # Handle multi_value field
        if isinstance(value, dict) and value.get("type") == "multi_value":
            doc_type_operator = value.get("operator")
            values = value.get("values", [])

            if doc_type_operator == "AND":
                # Add each value as a separate condition with `match`
                for v in values:
                    base_conditions.append({
                        "field": key,
                        "operator": "match",
                        "value": v
                    })
            elif doc_type_operator == "OR":
                # Add separate group for `OR` condition
                or_group = {
                    "id": "mainGroup",
                    "operator": "AND",
                    "conditions": [
                        {
                            "field": key,
                            "operator": "in",
                            "value": values
                        }
                    ]
                }

        else:
            base_conditions.append({
                "field": key,
                "operator": resolve_operator(value) ,
                "value": value
            })

    # Add main group for base conditions
    main_groups.append({
        "id": "mainGroup",
        "operator": operator,
        "conditions": base_conditions
    })

    # If doc_type is OR, append separate group
    if 'doc_type' in result and result['doc_type'].get('type') == 'multi_value' and result['doc_type'].get('operator') == 'OR':
        main_groups.append(or_group)

    return {
        "index": index,
        "main_groups": main_groups,
        "nested_groups": [],
        "inner_groups": []
    }

def resolve_operator(value):
    # Return "match" for strings, "==" for all other types
    return "match" if isinstance(value, str) else "=="


def transform_query_v1(input_data):
    # Start the output with index name
    output = {
        "index_name": input_data["index"],
        "query": {
            "operator": "AND",
            "groups": []
        },
        "pagination": {
            "from": 0,
            "size": 10
        },
        "sort": [
            {
                "field": "customer_id",
                "order": "desc"
            }
        ]
    }

    main_groups = input_data.get("main_groups", [])

    if len(main_groups) == 1:
        # Simple structure: just map directly
        group = {
            "operator": main_groups[0]["operator"],
            "conditions": main_groups[0]["conditions"]
        }
        output["query"]["groups"].append(group)

    elif len(main_groups) > 1:
        # Nest additional groups inside first group
        outer = {
            "operator": main_groups[0]["operator"],
            "conditions": main_groups[0].get("conditions", []),
            "groups": []
        }
        for sub_group in main_groups[1:]:
            nested = {
                "operator": sub_group["operator"],
                "conditions": sub_group.get("conditions", [])
            }
            outer["groups"].append(nested)
        output["query"]["groups"].append(outer)

    return output

def build_field_query_clause(field_name: str, field_value: Any, field_config: Dict) -> Optional[Dict]:
    """
    Build Elasticsearch query clause for individual field
    """
    input_type = field_config.get('inputType', 'text')
    field_role = field_config.get('role', 'value')

    # Handle multi-value fields (enhanced checkboxes)
    if isinstance(field_value, dict) and field_value.get('type') == 'multi_value':
        return build_multi_value_query(field_name, field_value, field_config)

    # Handle different input types
    if input_type == 'text':
        if field_role == 'key':
            # Exact match for key fields
            return {"term": {f"{field_name}": field_value}}
        else:
            # Fuzzy text search
            return {
                "multi_match": {
                    "query": field_value,
                    "fields": [field_name, f"{field_name}"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            }

    elif input_type == 'number':
        return {"term": {field_name: field_value}}

    elif input_type == 'number-range':
        if isinstance(field_value, dict):
            return {"range": {field_name: field_value}}
        else:
            # Single value treated as term
            return {"term": {field_name: field_value}}

    elif input_type == 'date':
        return build_date_query(field_name, field_value, field_config)

    elif input_type == 'date-range':
        if isinstance(field_value, dict):
            range_query = dict(field_value)
            range_query.setdefault("format", "yyyy-MM-dd")
            return {"range": {field_name: range_query}}
        else:
            return build_date_query(field_name, field_value, field_config)

    elif input_type == 'dropdown':
        return {"term": {f"{field_name}": field_value}}

    elif input_type == 'radio':
        return {"term": {field_name: field_value}}

    elif input_type == 'checkbox':
        # Handle regular checkboxes (array values)
        if isinstance(field_value, list):
            return {"terms": {field_name: field_value}}
        else:
            return {"term": {field_name: field_value}}

    else:
        # Default to term query
        return {"term": {field_name: field_value}}

def build_multi_value_query(field_name: str, field_data: Dict, field_config: Dict) -> Dict:
    """
    Build query for multi-value fields with logical operators
    """
    operator = field_data.get('operator', 'AND')
    values = field_data.get('values', [])

    if not values:
        return None

    # Build individual term queries
    term_queries = [{"term": {f"{field_name}": value}} for value in values]

    # Apply logical operator
    if operator == 'AND':
        return {
            "bool": {
                "must": term_queries
            }
        }
    elif operator == 'OR':
        return {
            "bool": {
                "should": term_queries,
                "minimum_should_match": 1
            }
        }
    elif operator == 'NOT':
        return {
            "bool": {
                "must_not": term_queries
            }
        }
    else:
        # Default to OR
        return {"terms": {f"{field_name}": values}}

def build_date_query(field_name: str, field_value: str, field_config: Dict) -> Dict:
    """
    Build date range query
    """
    try:
        # Handle date range if value contains special syntax
        if ' TO ' in field_value:
            start_date, end_date = field_value.split(' TO ')
            return {
                "range": {
                    field_name: {
                        "gte": start_date.strip(),
                        "lte": end_date.strip(),
                        "format": "yyyy-MM-dd||yyyy-MM-dd HH:mm:ss"
                    }
                }
            }
        else:
            # Single date - match the entire day
            return {
                "range": {
                    field_name: {
                        "gte": field_value,
                        "lt": field_value + "||+1d",
                        "format": "yyyy-MM-dd"
                    }
                }
            }
    except Exception:
        # Fallback to exact match
        return {"term": {field_name: field_value}}

def build_search_aggregations(form_config: Dict) -> Dict:
    """
    Build aggregations for faceted search
    """
    aggs = {}

    for field_name, field_config in form_config.get('fields', {}).items():
        input_type = field_config.get('inputType')

        # Add aggregations for facetable fields
        if input_type in ['dropdown', 'checkbox', 'radio']:
            aggs[f"{field_name}_facet"] = {
                "terms": {
                    "field": f"{field_name}",
                    "size": 10
                }
            }
        elif input_type in ['date', 'date-range']:
            aggs[f"{field_name}_histogram"] = {
                "date_histogram": {
                    "field": field_name,
                    "calendar_interval": "month"
                }
            }

    return aggs

async def execute_elasticsearch_query(form_config: Dict, query_body: Dict) -> Dict:
    """
    Execute the query against Elasticsearch
    """
    try:
        # Get environment configuration
        env_id = form_config.get('environment')
        index_name = form_config.get('index_name')

        if not env_id or not index_name:
            raise Exception("Missing environment or index configuration")

        # Get Elasticsearch environment details
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)

        if not env:
            raise Exception(f"Environment {env_id} not found")

        # Prepare request URL
        es_url = f"{env['host_url']}/{index_name}/_search"

        # Setup authentication
        auth = None
        if env.get('username') and env.get('password'):
            auth = (env['username'], env['password'])

        # Execute query
        response = requests.post(
            es_url,
            json=query_body,
            auth=auth,
            timeout=30,
            verify=False
        )

        if response.status_code != 200:
            raise Exception(f"Elasticsearch query failed: {response.status_code} - {response.text}")

        result = response.json()

        # Format response
        formatted_result = {
            "hits": [
                {
                    "id": hit.get("_id"),
                    "score": hit.get("_score"),
                    "data": hit.get("_source", {}),
                    "highlight": hit.get("highlight", {})
                }
                for hit in result.get("hits", {}).get("hits", [])
            ],
            "total": result.get("hits", {}).get("total", {}).get("value", 0),
            "took": result.get("took", 0),
            "aggregations": result.get("aggregations", {})
        }

        return formatted_result

    except Exception as e:
        print(f"Elasticsearch query error: {str(e)}")
        raise Exception(f"Search execution failed: {str(e)}")

def log_form_submission(form_url: str, submission_data: FormSubmissionData, result: Dict):
    """
    Log form submission for analytics (optional)
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            # Create submissions log table if it doesn't exist
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS form_submissions (
                                                                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                           form_url TEXT NOT NULL,
                                                                           fields_data TEXT NOT NULL,
                                                                           result_count INTEGER,
                                                                           response_time INTEGER,
                                                                           user_agent TEXT,
                                                                           ip_address TEXT,
                                                                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                           )
                           ''')

            # Insert submission log
            cursor.execute('''
                           INSERT INTO form_submissions
                               (form_url, fields_data, result_count, response_time, user_agent)
                           VALUES (?, ?, ?, ?, ?)
                           ''', (
                               form_url,
                               json.dumps(submission_data.fields),
                               result.get("total", 0),
                               result.get("took", 0),
                               submission_data.metadata.get("userAgent", "")
                           ))

            conn.commit()

    except Exception as e:
        print(f"Failed to log form submission: {str(e)}")
        # Don't fail the request if logging fails

# Enhanced field values endpoint with filtering and pagination
@app.get("/field-values_v1/{env_id}/{index_name}/{field_name}")
async def get_enhanced_field_values_v1(
        env_id: int,
        index_name: str,
        field_name: str,
        search: Optional[str] = None,
        limit: int = 100
):
    """
    Get field values with search filtering and pagination
    """
    try:
        # Get environment
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Build aggregation query
        query = {
            "size": 0,
            "aggs": {
                "unique_values": {
                    "terms": {
                        "field": f"{field_name}.keyword" if not field_name.endswith('.keyword') else field_name,
                        "size": limit,
                        "order": {"_count": "desc"}
                    }
                }
            }
        }

        # Add search filter if provided
        if search:
            query["query"] = {
                "wildcard": {
                    f"{field_name}.keyword": f"*{search.lower()}*"
                }
            }

        # Execute query
        if not env['host_url'].startswith(('http://', 'https://')):
            env['host_url'] = f"http://{env['host_url']}"

        auth = None
        if env.get('username') and env.get('password'):
            auth = (env['username'], env['password'])

        response = requests.post(
            f"{env['host_url']}/{index_name}/_search",
            json=query,
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            result = response.json()
            buckets = result.get('aggregations', {}).get('unique_values', {}).get('buckets', [])

            values = [
                {
                    'value': bucket['key'],
                    'count': bucket['doc_count'],
                    'percentage': round((bucket['doc_count'] / max(1, sum(b['doc_count'] for b in buckets))) * 100, 1)
                }
                for bucket in buckets
            ]
            print(values)
            return {
                "success": True,
                "values": values,
                "total_count": len(values),
                "search_term": search,
                "field_name": field_name
            }
        else:
            raise Exception(f"Elasticsearch query failed: {response.status_code}")

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "values": [],
            "total_count": 0
        }







def build_query_v5(result: dict, operator: str, index: str):
    """
    Build a query structure from input parameters.
    Args:
        result: Dictionary containing query parameters
        operator: Main operator for combining conditions
        index: Index name for the query
    Returns:
        Dictionary containing the structured query
    """
    main_groups = []
    base_conditions = []
    nested_groups = []
    or_group = None  # Initialize to avoid UnboundLocalError

    for key, value in result.items():
        # Check if field is nested (contains dot)
        is_nested = '.' in key

        # Handle multi_value field
        if isinstance(value, dict) and value.get("type") == "multi_value":
            key = key+".keyword"
            doc_type_operator = value.get("operator")
            values = value.get("values", [])

            if doc_type_operator == "AND":
                # Add each value as a separate condition with `match`
                for v in values:
                    condition = create_condition(key, "match", v, is_nested)
                    if is_nested:
                        add_to_nested_groups_v1(nested_groups, condition)
                    else:
                        base_conditions.append(condition)
            elif doc_type_operator == "OR":
                # Create OR group for this field
                condition = create_condition(key, "in", values, is_nested)
                if is_nested:
                    # For nested fields, always create a new nested group
                    nested_path = key.split('.')[0]
                    nested_groups.append({
                        "nested_path": nested_path,
                        "operator": "OR",
                        "conditions": [condition]
                    })
                else:
                    # For regular fields, create OR group
                    or_group = {
                        "id": f"{key}_group",
                        "operator": "OR",
                        "conditions": [condition]
                    }
        else:
            # Handle regular fields
            condition = create_condition(key, resolve_operator_v1(value), value, is_nested)
            if is_nested:
                add_to_nested_groups_v1(nested_groups, condition)
            else:
                base_conditions.append(condition)

    # Add main group for base conditions
    if base_conditions:  # Only add if there are conditions
        main_groups.append({
            "id": "mainGroup",
            "operator": operator,
            "conditions": base_conditions
        })

    # Add OR group if it was created
    if or_group is not None:
        main_groups.append(or_group)

    return {
        "index": index,
        "main_groups": main_groups,
        "nested_groups": nested_groups,
        "inner_groups": []
    }



def build_query_v3(result: dict, operator: str, index: str):
    """
    Build a query structure from input parameters.

    Args:
        result: Dictionary containing query parameters
        operator: Main operator for combining conditions
        index: Index name for the query

    Returns:
        Dictionary containing the structured query
    """
    main_groups = []
    base_conditions = []
    nested_groups = []
    or_group = None  # Initialize to avoid UnboundLocalError

    for key, value in result.items():
        # Check if field is nested (contains dot)
        is_nested = '.' in key

        # Handle multi_value field
        if isinstance(value, dict) and value.get("type") == "multi_value":
            doc_type_operator = value.get("operator")
            values = value.get("values", [])

            if doc_type_operator == "AND":
                # Add each value as a separate condition with `match`
                for v in values:
                    condition = create_condition(key, "match", v, is_nested)
                    if is_nested:
                        add_to_nested_groups(nested_groups, condition)
                    else:
                        base_conditions.append(condition)
            elif doc_type_operator == "OR":
                # Create OR group for this field
                condition = create_condition(key, "in", values, is_nested)

                if is_nested:
                    # For nested fields, add to nested groups with OR operator
                    nested_path = key.split('.')[0]

                    # Find existing nested group or create new one
                    existing_group = None
                    for group in nested_groups:
                        if group["nested_path"] == nested_path:
                            existing_group = group
                            break

                    if existing_group:
                        # Add to existing group but change operator to OR if needed
                        existing_group["conditions"].append(condition)
                        # If we have multiple conditions in this group, we might need OR
                        if len(existing_group["conditions"]) > 1:
                            existing_group["operator"] = "OR"
                    else:
                        # Create new nested group with OR operator for multi-value
                        nested_groups.append({
                            "nested_path": nested_path,
                            "operator": "OR",  # Use OR for multi-value fields
                            "conditions": [condition]
                        })
                else:
                    # For regular fields, create OR group
                    or_group = {
                        "id": f"{key}_group",
                        "operator": "OR",  # Change to OR for multi-value OR fields
                        "conditions": [condition]
                    }
        else:
            # Handle regular fields
            condition = create_condition(key, resolve_operator(value), value, is_nested)
            if is_nested:
                add_to_nested_groups(nested_groups, condition)
            else:
                base_conditions.append(condition)

    # Add main group for base conditions
    if base_conditions:  # Only add if there are conditions
        main_groups.append({
            "id": "mainGroup",
            "operator": operator,
            "conditions": base_conditions
        })

    # Add OR group if it was created
    if or_group is not None:
        main_groups.append(or_group)

    return {
        "index": index,
        "main_groups": main_groups,
        "nested_groups": nested_groups,
        "inner_groups": []
    }


def create_condition(field: str, operator: str, value, is_nested: bool = False):
    """
    Create a condition object, handling nested fields.

    Args:
        field: Field name (may contain dots for nested fields)
        operator: Query operator
        value: Field value
        is_nested: Whether this is a nested field

    Returns:
        Dictionary representing the condition
    """
    # For both nested and regular fields, use the same structure
    # For nested fields, keep the full field path in the field name
    return {
        "field": field.split("#")[1] if "#" in field else field,  # Keep full path like "addresses.pincode"
        "operator": operator,
        "value": value
    }
def add_to_nested_groups_v1(nested_groups: list, condition: dict):
    """
    Add a condition to nested groups, creating separate groups for each condition.
    Args:
        nested_groups: List of nested groups
        condition: Condition to add
    """
    field = condition["field"]
    nested_path = field.split('.')[0]

    # Always create a new nested group for each condition
    nested_groups.append({
        "nested_path": nested_path,
        "operator": "AND",  # Default to AND for individual conditions
        "conditions": [condition]
    })


def add_to_nested_groups(nested_groups: list, condition: dict):
    """
    Add a nested condition to the appropriate nested group.

    Args:
        nested_groups: List of existing nested groups
        condition: Nested condition to add
    """
    # Extract nested path from the full field name
    field_name = condition["field"]
    nested_path = field_name.split('.')[0]  # Get root nested object

    # Find existing group for this nested path
    existing_group = None
    for group in nested_groups:
        if group["nested_path"] == nested_path:
            existing_group = group
            break

    if existing_group:
        # Add to existing group
        existing_group["conditions"].append(condition)
    else:
        # Create new nested group
        nested_groups.append({
            "nested_path": nested_path,
            "operator": "AND",
            "conditions": [condition]
        })


def resolve_operator_v1(value):
    """
    Resolve the operator based on value type.
    Args:
        value: The value to determine operator for
    Returns:
        String representing the operator
    """
    if isinstance(value, list):
        return "in"
    elif isinstance(value, dict):
        # Handle range queries, exists queries, etc.
        if "gte" in value or "lte" in value or "gt" in value or "lt" in value:
            return "range"
        elif "exists" in value:
            return "exists"
        else:
            return "match"
    else:
        return "match"

def resolve_operator(value):
    """
    Determine the appropriate operator based on value type.

    Args:
        value: The value to determine operator for

    Returns:
        String representing the operator
    """
    if isinstance(value, str):
        return "match"
    elif isinstance(value, list):
        return "in"
    elif isinstance(value, (int, float)):
        return "=="
    else:
        return "=="


def transform_query_v3(input_data):
    """
    Transform query data into final query structure.

    Args:
        input_data: Dictionary containing query structure

    Returns:
        Dictionary in final query format
    """
    if not input_data or "index" not in input_data:
        raise ValueError("Input data must contain 'index' field")

    # Start the output with index name
    output = {
        "index_name": input_data["index"],
        "query": {
            "operator": "AND",
            "groups": []
        },
        "pagination": {
            "from": 0,
            "size": 10
        },
        "sort": [
            {

                "order": "desc"
            }
        ]
    }

    main_groups = input_data.get("main_groups", [])
    nested_groups = input_data.get("nested_groups", [])

    # Handle main groups
    if len(main_groups) == 0:
        # No main groups - check for nested only
        pass
    elif len(main_groups) == 1:
        # Simple structure: just map directly
        group = {
            "operator": main_groups[0]["operator"],
            "conditions": main_groups[0].get("conditions", [])
        }
        output["query"]["groups"].append(group)
    else:
        # Multiple groups: nest additional groups inside first group
        first_group = main_groups[0]
        outer = {
            "operator": first_group["operator"],
            "conditions": first_group.get("conditions", []),
            "groups": []
        }

        # Add remaining groups as nested groups
        for sub_group in main_groups[1:]:
            nested = {
                "operator": sub_group["operator"],
                "conditions": sub_group.get("conditions", [])
            }
            outer["groups"].append(nested)

        output["query"]["groups"].append(outer)

    # Handle standalone nested groups (not part of main groups)
    if nested_groups:
        # Add nested groups as separate top-level groups
        for nested_group in nested_groups:
            nested_query_group = {
                "operator": nested_group["operator"],
                "nested_path": nested_group["nested_path"],
                "conditions": nested_group["conditions"]  # Use conditions instead of nested_conditions
            }
            output["query"]["groups"].append(nested_query_group)

    return output



def transform_query_v5(input_data):
    """
    Transform query data into final query structure.

    Args:
        input_data: Dictionary containing query structure

    Returns:
        Dictionary in final query format
    """
    if not input_data or "index" not in input_data:
        raise ValueError("Input data must contain 'index' field")

    # Start the output with index name
    output = {
        "index_name": input_data["index"],
        "query": {
            "operator": "AND",
            "groups": []
        },
        "pagination": {
            "from": 0,
            "size": 10
        },
        "sort": [
            {
                "order": "desc"
            }
        ]
    }

    main_groups = input_data.get("main_groups", [])
    nested_groups = input_data.get("nested_groups", [])

    # Handle main groups
    if len(main_groups) == 0:
        # No main groups - check for nested only
        pass
    elif len(main_groups) == 1:
        # Simple structure: just map directly
        group = {
            "operator": main_groups[0]["operator"],
            "conditions": main_groups[0].get("conditions", [])
        }
        output["query"]["groups"].append(group)
    else:
        # Multiple groups: nest additional groups inside first group
        first_group = main_groups[0]
        outer = {
            "operator": first_group["operator"],
            "conditions": first_group.get("conditions", []),
            "groups": []
        }

        # Add remaining groups as nested groups
        for sub_group in main_groups[1:]:
            nested = {
                "operator": sub_group["operator"],
                "conditions": sub_group.get("conditions", [])
            }
            outer["groups"].append(nested)

        output["query"]["groups"].append(outer)

    # Handle standalone nested groups (not part of main groups)
    if nested_groups:
        for nested_group in nested_groups:
            nested_operator = nested_group["operator"]
            nested_path = nested_group["nested_path"]
            conditions = nested_group.get("conditions", [])

            # Special handling: If nested group has AND operator and multiple conditions,
            # create separate nested groups for each condition
            if nested_operator == "AND" and len(conditions) > 1:
                # Split each condition into its own nested group
                for condition in conditions:
                    individual_nested_group = {
                        "operator": "AND",  # Each individual group uses AND
                        "nested_path": nested_path,
                        "conditions": [condition]  # Single condition per group
                    }
                    output["query"]["groups"].append(individual_nested_group)
            else:
                # Keep existing behavior for non-AND operators or single conditions
                nested_query_group = {
                    "operator": nested_operator,
                    "nested_path": nested_path,
                    "conditions": conditions
                }
                output["query"]["groups"].append(nested_query_group)

    return output


@app.post("/api/validate-mapping", response_model=MappingValidationResponse)
async def validate_elasticsearch_mapping_v1(request: ElasticsearchMappingRequest):
    return await validate_elasticsearch_mapping(request)

@app.get("/oracle/workflow-tables/{env_id}")
async def get_workflow_tables(env_id: int):
    """Get tables for workflow - fetches all user tables from Oracle database"""
    try:
        environments = get_oracle_environments()  # You need this helper function
        oracle_env = next((e for e in environments if e['id'] == env_id), None)

        if not oracle_env:
            raise HTTPException(status_code=404, detail="Oracle environment not found")

        # Use correct variable names and proper indentation
        connection = oracledb.connect(
            user=oracle_env['username'],
            password=oracle_env['password'],
            dsn=oracle_env['url']
        )

        query = """
                SELECT
                    table_name,
                    num_rows,
                    last_analyzed,
                    tablespace_name
                FROM user_tables
                WHERE table_name NOT LIKE 'BIN$%'  -- Exclude recycle bin tables
                ORDER BY table_name \
                """

        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        tables = []
        for row in rows:
            table_info = {
                "table_name": row[0],
                "num_rows": row[1] if row[1] is not None else 0,
                "last_analyzed": row[2].isoformat() if row[2] else None,
                "tablespace_name": row[3]
            }
            tables.append(table_info)

        cursor.close()
        connection.close()

        return {
            "success": True,
            "tables": tables,
            "total_count": len(tables)
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "tables": []
        }





@app.post("/oracle/auto-detect-relationships/{env_id}")
async def auto_detect_relationships(env_id: int, query: str = Form(...), tables: str = Form(...)):
    try:
        selected_tables = json.loads(tables)
        logger.info(f"Selected tables: {selected_tables}")
        logger.info(f"Input query: {query}")

        cleaned_query = clean_sql_query(query)
        logger.info(f"Cleaned query: {cleaned_query}")

        relationships = []

        # Detect relationships from JOINs
        join_relationships = detect_join_relationships(cleaned_query, selected_tables)
        relationships.extend(join_relationships)

        # Detect relationships from WHERE clauses
        where_relationships = detect_where_relationships(cleaned_query, selected_tables)
        relationships.extend(where_relationships)

        # Remove duplicates
        relationships = remove_duplicate_relationships(relationships)

        logger.info(f"Detected relationships: {relationships}")

        return JSONResponse({
            "success": True,
            "relationships": relationships,
            "query_analyzed": cleaned_query,
            "total_relationships": len(relationships)
        })

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return JSONResponse({"success": False, "error": f"Invalid JSON in tables parameter: {str(e)}"})
    except Exception as e:
        logger.error(f"Error in auto_detect_relationships: {str(e)}", exc_info=True)
        return JSONResponse({"success": False, "error": f"Analysis failed: {str(e)}"})

def clean_sql_query(query: str) -> str:
    """Clean SQL query by removing comments and normalizing whitespace."""
    # Remove single-line comments
    query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    # Remove multi-line comments
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
    # Normalize whitespace and convert to uppercase
    query = ' '.join(query.split()).upper().strip()
    return query

def extract_aliases(query: str, selected_tables: List[str]) -> Dict[str, str]:
    """Extract table aliases from SQL query."""
    aliases = {}

    # Pattern to match FROM clause with potential aliases
    from_pattern = r'FROM\s+((?:\w+(?:\s+\w+)?(?:\s*,\s*)?)+)(?:\s+(?:INNER|LEFT|RIGHT|FULL|WHERE|ORDER|GROUP|HAVING|LIMIT)|\s*$)'
    from_match = re.search(from_pattern, query, flags=re.IGNORECASE)

    if from_match:
        from_clause = from_match.group(1).strip()
        # Split by comma to handle multiple tables in FROM
        tables = [t.strip() for t in from_clause.split(',')]

        for table_expr in tables:
            # Handle "table alias" or "table AS alias" patterns
            as_pattern = r'(\w+)(?:\s+AS\s+(\w+)|\s+(\w+))?'
            match = re.match(as_pattern, table_expr.strip(), flags=re.IGNORECASE)
            if match:
                table_name = match.group(1).upper()
                alias = (match.group(2) or match.group(3) or table_name).upper()
                aliases[alias] = table_name

    # Pattern to match JOIN clauses
    join_pattern = r'(?:INNER|LEFT|RIGHT|FULL)?\s*JOIN\s+(\w+)(?:\s+AS\s+(\w+)|\s+(\w+))?'
    for match in re.finditer(join_pattern, query, flags=re.IGNORECASE):
        table_name = match.group(1).upper()
        alias = (match.group(2) or match.group(3) or table_name).upper()
        aliases[alias] = table_name

    logger.info(f"Extracted aliases: {aliases}")
    return aliases

def resolve_alias_to_table(alias: str, alias_map: Dict[str, str]) -> str:
    """Resolve alias to actual table name."""
    if not alias:
        return None
    return alias_map.get(alias.upper())

def detect_join_relationships(query: str, selected_tables: List[str]) -> List[Dict[str, Any]]:
    """Detect relationships from JOIN clauses."""
    relationships = []
    alias_map = extract_aliases(query, selected_tables)

    # More flexible JOIN pattern that handles various formats
    join_patterns = [
        # Standard JOIN with ON clause
        r'(?:INNER|LEFT|RIGHT|FULL)?\s*JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
        # JOIN with table names in different order
        r'(?:INNER|LEFT|RIGHT|FULL)?\s*JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
    ]

    for pattern in join_patterns:
        matches = re.findall(pattern, query, flags=re.IGNORECASE)

        for match in matches:
            if len(match) == 6:
                table_joined, alias_joined, left_alias, left_field, right_alias, right_field = match

                # Handle case where alias might be empty
                if not alias_joined:
                    alias_joined = table_joined

                left_table = resolve_alias_to_table(left_alias, alias_map)
                right_table = resolve_alias_to_table(right_alias, alias_map)

                if not left_table or not right_table:
                    logger.warning(f"Could not resolve tables: {left_alias} -> {left_table}, {right_alias} -> {right_table}")
                    continue

                confidence = calculate_confidence(left_field, right_field, left_table, right_table)

                relationships.append({
                    'parentTable': left_table.lower(),
                    'parentField': left_field.lower(),
                    'childTable': right_table.lower(),
                    'childField': right_field.lower(),
                    'type': 'nested',
                    'confidence': confidence,
                    'detected_from': 'explicit_join'
                })

    return relationships

def detect_where_relationships(query: str, selected_tables: List[str]) -> List[Dict[str, Any]]:
    """Detect relationships from WHERE clause conditions."""
    relationships = []
    alias_map = extract_aliases(query, selected_tables)

    # Pattern to find table.column = table.column in WHERE/AND clauses
    where_patterns = [
        r'WHERE\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
        r'AND\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
        r'ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'  # Additional ON clauses not in JOINs
    ]

    for pattern in where_patterns:
        matches = re.findall(pattern, query, flags=re.IGNORECASE)
        for alias1, field1, alias2, field2 in matches:
            table1 = resolve_alias_to_table(alias1, alias_map)
            table2 = resolve_alias_to_table(alias2, alias_map)

            if table1 and table2 and table1 != table2:
                confidence = calculate_confidence(field1, field2, table1, table2)
                relationships.append({
                    'parentTable': table1.lower(),
                    'parentField': field1.lower(),
                    'childTable': table2.lower(),
                    'childField': field2.lower(),
                    'type': 'nested',
                    'confidence': confidence,
                    'detected_from': 'where_clause'
                })

    return relationships

def calculate_confidence(field1: str, field2: str, table1: str, table2: str) -> float:
    """Calculate confidence score for detected relationship."""
    confidence = 0.6  # Base confidence

    # Exact field name match increases confidence
    if field1.upper() == field2.upper():
        confidence += 0.25

    # ID fields are likely to be relationships
    if 'ID' in field1.upper() or 'ID' in field2.upper():
        confidence += 0.1

    # Fields ending with _ID are very likely to be foreign keys
    if field1.upper().endswith('_ID') or field2.upper().endswith('_ID'):
        confidence += 0.05

    # Common primary key patterns
    if field1.upper() in ['ID', 'PK'] or field2.upper() in ['ID', 'PK']:
        confidence += 0.05

    # Field name contains table name (e.g., customer_id in customer table)
    table1_clean = table1.replace('_', '').upper()
    table2_clean = table2.replace('_', '').upper()
    if (table1_clean in field1.upper() or table1_clean in field2.upper() or
            table2_clean in field1.upper() or table2_clean in field2.upper()):
        confidence += 0.05

    return min(confidence, 0.95)  # Cap at 95%

def remove_duplicate_relationships(relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate relationships, keeping the one with highest confidence."""
    seen = {}
    unique = []

    for rel in relationships:
        # Create key for both directions of the relationship
        key1 = (rel['parentTable'], rel['parentField'], rel['childTable'], rel['childField'])
        key2 = (rel['childTable'], rel['childField'], rel['parentTable'], rel['parentField'])

        # Check if we've seen this relationship in either direction
        existing_key = None
        if key1 in seen:
            existing_key = key1
        elif key2 in seen:
            existing_key = key2

        if existing_key:
            # Keep the relationship with higher confidence
            existing_rel = seen[existing_key]
            if rel['confidence'] > existing_rel['confidence']:
                # Replace with higher confidence relationship
                unique.remove(existing_rel)
                unique.append(rel)
                seen[existing_key] = rel
        else:
            # New relationship
            seen[key1] = rel
            unique.append(rel)

    return unique



@app.post("/oracle/generate-workflow-mapping/{env_id}")
async def generate_workflow_mapping(env_id: int, request: Request, dry_run: bool = False):
    """Generate mapping for multiple tables with relationships.

    If ``dry_run`` is true, the mapping is returned without saving or creating
    the Elasticsearch index, allowing clients to preview the result.
    """
    try:
        # Parse request data
        data = await request.json()
        mapping_name = data.get('mappingName', '').strip()
        index_name = data.get('indexName', '').strip()
        tables = data.get('tables', [])
        relationships = data.get('relationships', [])
        table_structures = data.get('tableStructures', {})
        field_mappings = data.get('fieldMappings', {})
        autoDetectionBtn = data.get('autoDetectionBtn', {})

        logger.info(f"🚀 Generating workflow mapping: {mapping_name}")
        logger.info(f"📊 Tables: {tables}")
        logger.info(f"🔗 Relationships: {len(relationships)}")

        # Validation
        if not mapping_name or not index_name:
            raise ValueError("Mapping name and index name are required")

        if not tables:
            raise ValueError("At least one table must be selected")

        if not relationships:
            raise ValueError("At least one relationship must be defined")

        # Check if mapping name already exists
        if mapping_exists(mapping_name):
            raise ValueError(f"Mapping with name '{mapping_name}' already exists")

        # Generate Elasticsearch mapping
        elasticsearch_mapping = generate_elasticsearch_mapping_v1(
            tables, relationships, table_structures, field_mappings
        )

        total_fields = count_mapping_fields(elasticsearch_mapping)
        logger.info(f"📝 Generated mapping with {total_fields} fields")

        if dry_run:
            return {
                "success": True,
                "mappingName": mapping_name,
                "indexName": index_name,
                "mapping": elasticsearch_mapping,
                "totalFields": total_fields,
                "status": "preview",
            }

        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)

        # Create index in Elasticsearch
        es_success = False
        es_error = None

        try:
            es_success = create_elasticsearch_index_v2(env['host_url'], index_name, elasticsearch_mapping, env.get('username'), env.get('password'))
        except Exception as e:
            es_error = str(e)
            logger.info(f"❌ Elasticsearch creation failed: {es_error}")

        # Save to SQLite database
        mapping_id = save_workflow_mapping(
            mapping_name=mapping_name,
            index_name=index_name,
            environment_id=env_id,
            tables=tables,
            relationships=relationships,
            elasticsearch_mapping=elasticsearch_mapping,
            table_structures=table_structures,
            elasticsearch_created=es_success,
            error_message=es_error
        )

        # Prepare response
        response_data = {
            "success": True,
            "mappingId": mapping_id,
            "mappingName": mapping_name,
            "indexName": index_name,
            "mapping": elasticsearch_mapping,
            "totalFields": total_fields,
            "tablesProcessed": len(tables),
            "relationshipsApplied": len(relationships),
            "elasticsearchCreated": es_success,
            "status": "completed" if es_success else "mapping_saved_es_failed",
            "message": "Mapping generated and saved successfully" +
                       (" (Elasticsearch index created)" if es_success else " (Elasticsearch creation failed)")
        }

        if es_error and not es_success:
            response_data["elasticsearchError"] = es_error

        logger.info(f"✅ Workflow mapping completed: {mapping_name}")

        return response_data

    except ValueError as e:
        logger.info(f"❌ Validation error: {e}")
        return {"success": False, "error": f"Validation error: {str(e)}"}
    except Exception as e:
        logger.info(f"❌ Unexpected error: {e}")
        return {"success": False, "error": f"Unexpected error: {str(e)}"}


def oracle_to_elastic_type(oracle_type: str) -> str:
    """Convert Oracle type to Elasticsearch type"""
    type_mapping = {
        'VARCHAR2': 'text',
        'CHAR': 'keyword',
        'NUMBER': 'long',
        'DATE': 'date',
        'TIMESTAMP': 'date',
        'CLOB': 'text',
        'BLOB': 'binary',
        'INTEGER': 'long',
        'FLOAT': 'double'
    }
    return type_mapping.get(oracle_type.upper(), 'text')




@app.get("/workflow-mappings/{mapping_id}")
async def get_workflow_mapping(mapping_id: int):
    """Get specific workflow mapping by ID"""
    try:
        conn = sqlite3.connect('workflow_mappings.db')
        cursor = conn.cursor()

        cursor.execute('''
                       SELECT * FROM workflow_mappings WHERE id = ? AND status = 'active'
                       ''', (mapping_id,))

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Mapping not found")

        mapping = {
            "id": row[0],
            "mapping_name": row[1],
            "index_name": row[2],
            "environment_id": row[3],
            "tables": json.loads(row[4]),
            "relationships": json.loads(row[5]),
            "elasticsearch_mapping": json.loads(row[6]),
            "table_structures": json.loads(row[7]),
            "total_fields": row[8],
            "status": row[9],
            "created_at": row[10],
            "updated_at": row[11],
            "elasticsearch_created": row[12],
            "error_message": row[13]
        }

        conn.close()
        return {"success": True, "mapping": mapping}

    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}



def save_workflow_mapping(mapping_name: str, index_name: str, environment_id: int,
                          tables: List[str], relationships: List[Dict],
                          elasticsearch_mapping: Dict, table_structures: Dict,
                          elasticsearch_created: bool = False, error_message: str = None,oracle_query: str = None) -> int:
    """Save workflow mapping to SQLite database"""
    try:
        conn = sqlite3.connect('workflow_mappings.db')
        cursor = conn.cursor()

        # Calculate total fields
        total_fields = count_mapping_fields(elasticsearch_mapping)
        print(error_message)
        print(elasticsearch_created)
        elasticsearch_created = bool(elasticsearch_created.get("created")) if isinstance(elasticsearch_created, dict) else bool(elasticsearch_created)
        # Insert mapping record
        cursor.execute('''
                       INSERT INTO workflow_mappings (
                           mapping_name, index_name, environment_id, tables, relationships,
                           elasticsearch_mapping, table_structures, total_fields, status,
                           elasticsearch_created, error_message, created_at, updated_at,oracle_query
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)
                       ''', (
                           mapping_name,
                           index_name,
                           environment_id,
                           json.dumps(tables),
                           json.dumps(relationships),
                           json.dumps(elasticsearch_mapping),
                           json.dumps(table_structures),
                           total_fields,
                           'active',
                           int(elasticsearch_created),
                           str(error_message) if error_message else None,
                           datetime.now().isoformat(),
                           datetime.now().isoformat(),
                           oracle_query
                       ))

        mapping_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"💾 Saved mapping to database with ID: {mapping_id}")
        return mapping_id

    except Exception as e:
        logger.info(f"❌ Error saving mapping to database: {e}")
        raise

@app.get("/workflow-mappings")
async def get_workflow_mappings():
    """Get all saved workflow mappings"""
    try:
        conn = sqlite3.connect('workflow_mappings.db')
        cursor = conn.cursor()

        cursor.execute('''
                       SELECT id, mapping_name, index_name, environment_id, tables,
                              total_fields, status, elasticsearch_created, created_at
                       FROM workflow_mappings
                       WHERE status = 'active'
                       ORDER BY created_at DESC
                       ''')

        mappings = []
        for row in cursor.fetchall():
            mappings.append({
                "id": row[0],
                "mapping_name": row[1],
                "index_name": row[2],
                "environment_id": row[3],
                "tables": json.loads(row[4]),
                "total_fields": row[5],
                "status": row[6],
                "elasticsearch_created": row[7],
                "created_at": row[8]
            })

        conn.close()
        return {"success": True, "mappings": mappings}

    except Exception as e:
        return {"success": False, "error": str(e)}

@app.delete("/workflow-mappings/{mapping_id}")
async def delete_workflow_mapping(mapping_id: int):
    """Delete workflow mapping (soft delete)"""
    try:
        conn = sqlite3.connect('workflow_mappings.db')
        cursor = conn.cursor()

        cursor.execute('''
                       UPDATE workflow_mappings
                       SET status = 'deleted', updated_at = ?
                       WHERE id = ? AND status = 'active'
                       ''', (datetime.now().isoformat(), mapping_id))

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Mapping not found")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Mapping deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}


def init_workflow_mappings_db():
    """Initialize SQLite database for storing workflow mappings"""
    conn = sqlite3.connect('workflow_mappings.db')
    cursor = conn.cursor()

    # Create workflow_mappings table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS workflow_mappings (
                                                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                    mapping_name TEXT NOT NULL UNIQUE,
                                                                    index_name TEXT NOT NULL,
                                                                    environment_id INTEGER NOT NULL,
                                                                    tables TEXT NOT NULL,  -- JSON array of table names
                                                                    
                                                                    relationships TEXT NOT NULL,  -- JSON array of relationships
                                                                    elasticsearch_mapping TEXT NOT NULL,  -- JSON mapping
                                                                    table_structures TEXT NOT NULL,  -- JSON of table structures
                                                                    total_fields INTEGER DEFAULT 0,
                                                                    status TEXT DEFAULT 'active',  -- active, deleted, error
                                                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                    elasticsearch_created BOOLEAN DEFAULT FALSE,
                                                                    error_message TEXT,
                                                                    oracle_query TEXT NOT NULL
                                                                    
                   )
                   ''')

    # Create index on mapping_name for faster lookups
    cursor.execute('''
                   CREATE INDEX IF NOT EXISTS idx_mapping_name ON workflow_mappings(mapping_name)
                   ''')

    # Create index on environment_id
    cursor.execute('''
                   CREATE INDEX IF NOT EXISTS idx_environment_id ON workflow_mappings(environment_id)
                   ''')

    # Create table for mapping updates with environment association
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS mapping_updates (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       env_id INTEGER NOT NULL,
                       index_name TEXT NOT NULL,
                       root_fields TEXT,
                       parent_child_fields TEXT,
                       parent_child_relation TEXT,
                       nested_fields TEXT,
                       ai_fields TEXT,
                       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                       UNIQUE(env_id, index_name)
                   )
                   ''')

    # Ensure legacy databases have env_id and root_fields columns and unique index
    cursor.execute("PRAGMA table_info(mapping_updates)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'env_id' not in columns:
        cursor.execute("ALTER TABLE mapping_updates ADD COLUMN env_id INTEGER NOT NULL DEFAULT 0")
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mapping_updates_env_idx ON mapping_updates(env_id, index_name)"
        )
    if 'root_fields' not in columns:
        cursor.execute("ALTER TABLE mapping_updates ADD COLUMN root_fields TEXT")
    if 'parent_child_relation' not in columns:
        cursor.execute("ALTER TABLE mapping_updates ADD COLUMN parent_child_relation TEXT")

    conn.commit()
    conn.close()

# Initialize database on startup
init_workflow_mappings_db()

def get_field_type(column) -> str:
    """Extract field type from column object"""
    if isinstance(column, dict):
        return (column.get('type') or
                column.get('data_type') or
                column.get('DATA_TYPE') or
                column.get('dataType') or
                'VARCHAR2')
    return 'VARCHAR2'

def count_mapping_fields(mapping: Dict) -> int:
    """Count total fields in the mapping"""
    properties = mapping.get("mappings", {}).get("properties", {})
    count = 0

    for field_name, field_config in properties.items():
        count += 1
        # Count nested properties
        if field_config.get("type") == "nested":
            nested_props = field_config.get("properties", {})
            count += len(nested_props)

    return count

def generate_elasticsearch_mapping_v1(tables: List[str], relationships: List[Dict], table_structures: Dict, field_mappings: Dict = None) -> Dict:
    """Generate complete Elasticsearch mapping from tables and relationships"""

    field_mappings = field_mappings or {}

    # 🆕 Aggregate overrides per table so type changes apply regardless of relationship order
    aggregated_overrides: Dict[str, Dict[str, str]] = {}
    if isinstance(field_mappings, dict):
        for idx, rel in enumerate(relationships):
            overrides = field_mappings.get(str(idx), {})
            parent_table = rel.get('parentTable')
            child_table = rel.get('childTable')

            parent_map = overrides.get('parent', {})
            child_map = overrides.get('child', {})

            if parent_table and parent_map:
                table_key = parent_table.lower()
                aggregated_overrides.setdefault(table_key, {})
                for fname, etype in parent_map.items():
                    aggregated_overrides[table_key][fname.lower()] = etype

            if child_table and child_map:
                table_key = child_table.lower()
                aggregated_overrides.setdefault(table_key, {})
                for fname, etype in child_map.items():
                    aggregated_overrides[table_key][fname.lower()] = etype

    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "standard_analyzer": {
                        "type": "standard",
                        "stopwords": "_english_"
                    }
                }
            }
        },
        "mappings": {
            "properties": {}
        }
    }

    # Use case-insensitive tracking of processed tables
    processed_tables = set()

    # Process parent tables first
    for relationship in relationships:
        parent_table = relationship.get('parentTable')
        child_table = relationship.get('childTable')
        rel_type = relationship.get('type', 'nested')
        parent_overrides = aggregated_overrides.get(parent_table.lower(), {}) if parent_table else {}
        child_overrides = aggregated_overrides.get(child_table.lower(), {}) if child_table else {}

        # Add parent table fields (normalize case for comparison)
        if parent_table and parent_table.upper() not in processed_tables:
            parent_columns = table_structures.get(parent_table, [])
            for column in parent_columns:
                field_name = get_field_name(column).lower()
                oracle_type = get_field_type(column)
                elastic_type = parent_overrides.get(field_name, oracle_to_elastic_type(oracle_type))

                mapping["mappings"]["properties"][field_name] = {
                    "type": elastic_type
                }

                # Add keyword sub-field for text fields (for aggregations)
                if elastic_type == "text":
                    mapping["mappings"]["properties"][field_name]["fields"] = {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }

            processed_tables.add(parent_table.upper())

        # Handle relationship-specific mapping
        if rel_type == 'nested' and child_table:
            # Create nested object for child table
            child_columns = table_structures.get(child_table, [])
            nested_properties = {}

            for column in child_columns:
                field_name = get_field_name(column).lower()
                oracle_type = get_field_type(column)
                elastic_type = child_overrides.get(field_name, oracle_to_elastic_type(oracle_type))

                nested_properties[field_name] = {"type": elastic_type}

                # Add keyword sub-field for text fields
                if elastic_type == "text":
                    nested_properties[field_name]["fields"] = {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }

            # Add nested field to mapping
            nested_field_name = f"{child_table.lower()}_items"
            mapping["mappings"]["properties"][nested_field_name] = {
                "type": "nested",
                "properties": nested_properties
            }

            # Mark child table as processed (normalize case)
            processed_tables.add(child_table.upper())

        elif rel_type in {'join', 'parent-child'}:
            relation_name = relationship.get('relationName', 'join_field')
            parent_type = relationship.get('parentType', (parent_table or '').lower())
            child_type = relationship.get('childType', (child_table or '').lower())

            join_field = mapping["mappings"]["properties"].get(relation_name, {
                "type": "join",
                "relations": {}
            })
            join_field["relations"][parent_type] = child_type
            mapping["mappings"]["properties"][relation_name] = join_field
            child_columns = table_structures.get(child_table, [])
            for column in child_columns:
                field_name = get_field_name(column).lower()
                oracle_type = get_field_type(column)
                elastic_type = child_overrides.get(field_name, oracle_to_elastic_type(oracle_type))
                mapping["mappings"]["properties"][field_name] = {"type": elastic_type}
                if elastic_type == "text":
                    mapping["mappings"]["properties"][field_name]["fields"] = {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
            processed_tables.add(child_table.upper())

    # Add any remaining tables that weren't processed through relationships
    for table_name in tables:
        if table_name.upper() not in processed_tables:  # Case-insensitive check
            table_columns = table_structures.get(table_name, [])
            for column in table_columns:
                field_name = get_field_name(column).lower()
                oracle_type = get_field_type(column)
                elastic_type = oracle_to_elastic_type(oracle_type)

                if field_name not in mapping["mappings"]["properties"]:
                    mapping["mappings"]["properties"][field_name] = {
                        "type": elastic_type
                    }

                    if elastic_type == "text":
                        mapping["mappings"]["properties"][field_name]["fields"] = {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }

    return mapping


def get_field_name(column) -> str:
    """Extract field name from column object"""
    if isinstance(column, dict):
        return (column.get('name') or
                column.get('column_name') or
                column.get('COLUMN_NAME') or
                column.get('columnName') or
                'unknown_field')
    return str(column)



def mapping_exists(mapping_name: str) -> bool:
    """Check if mapping name already exists"""
    try:
        conn = sqlite3.connect('workflow_mappings.db')
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) FROM workflow_mappings WHERE mapping_name = ? AND status = 'active'",
            (mapping_name,)
        )

        count = cursor.fetchone()[0]
        conn.close()

        return count > 0

    except Exception as e:
        print(f"Error checking mapping existence: {e}")
        return False


@app.post("/save-mapping-update")
async def save_mapping_update(update: MappingUpdate):
    """Save mapping update selections to SQLite"""
    try:
        with sqlite3.connect('workflow_mappings.db') as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO mapping_updates (
                    env_id, index_name, root_fields, parent_child_fields, parent_child_relation, nested_fields, ai_fields, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(env_id, index_name) DO UPDATE SET
                    root_fields=excluded.root_fields,
                    parent_child_fields=excluded.parent_child_fields,
                    parent_child_relation=excluded.parent_child_relation,
                    nested_fields=excluded.nested_fields,
                    ai_fields=excluded.ai_fields,
                    created_at=excluded.created_at
            ''', (
                update.env_id,
                update.index_name,
                json.dumps(update.root_fields),
                json.dumps(update.parent_child_fields),
                update.parent_child_relation,
                json.dumps(update.nested_fields),
                json.dumps(update.ai_fields),
                datetime.now().isoformat()
            ))
            conn.commit()
        return {"success": True}

    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/mapping-update/{env_id}/{index_name}")
async def get_mapping_update(env_id: int, index_name: str):
    root_fields, parent_fields, relation, nested_fields, ai_fields = fetch_field_lists(env_id, index_name)
    return {
        "root_fields": root_fields,
        "parent_child_fields": parent_fields,
        "parent_child_relation": relation,
        "nested_fields": nested_fields,
        "ai_fields": ai_fields,
    }
SELECT_SQL = """
             SELECT root_fields, parent_child_fields, parent_child_relation, nested_fields, ai_fields,parent_child_relation
             FROM mapping_updates
             WHERE env_id = ? AND index_name = ?
                 LIMIT 1; \
             """

def fetch_field_lists(env_id: int,index_name: str) -> Tuple[List[str], List[str], Optional[str], List[str], List[str]]:

    with sqlite3.connect('workflow_mappings.db') as conn:
        cur = conn.cursor()
        cur.execute(SELECT_SQL, (env_id, index_name))
        row = cur.fetchone()

    print("row",row)

    if not row:
        return [], [], None, [], [], []
    else:
        root_fields_raw, parent_child_fields_raw, relation_raw, nested_fields_raw, ai_fields_raw,parent_child_raw = row
        root_field_list = _extract_list(root_fields_raw)
        parent_field_list = _extract_list(parent_child_fields_raw)
        relation = relation_raw if relation_raw else None
        nested_field_list = _extract_list(nested_fields_raw)
        ai_field_list = _extract_list(ai_fields_raw)
        parent_child_list = _extract_list(parent_child_raw)
        return root_field_list, parent_field_list, relation, nested_field_list, ai_field_list,parent_child_list

def _extract_list(value: Any) -> List[str]:
    """
    Robustly turn stored JSON into a list[str].
    Accepts:
      - list[str]
      - dict with 'fields' or 'values'
      - JSON-encoded str of any of the above
      - None -> []
    """
    if value is None:
        return []

    # If DB returned raw text, parse it
    if isinstance(value, (bytes, str)):
        try:
            value = json.loads(value)
        except Exception:
            # Not valid JSON; treat as single string path
            s = value.decode() if isinstance(value, bytes) else value
            return [s] if s else []

    if isinstance(value, list):
        # Normalize to strings
        return [str(v) for v in value]

    if isinstance(value, dict):
        if "fields" in value and isinstance(value["fields"], list):
            return [str(v) for v in value["fields"]]
        if "values" in value and isinstance(value["values"], list):
            return [str(v) for v in value["values"]]
        # If dict is actually a map of {path: true/false}
        keys_as_paths = [k for k, v in value.items() if isinstance(k, str) and (v is True or isinstance(v, (str, int, float)))]
        if keys_as_paths:
            return keys_as_paths

    # Fallback: nothing usable
    return []



@app.get("/api/enhanced-indices/{env_id}")
async def get_enhanced_indices(env_id: int):
    """Get enhanced indices with detailed metadata"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Get indices with enhanced metadata
        indices = get_elasticsearch_indices_enhanced(
            env['host_url'],
            env.get('username'),
            env.get('password')
        )
        return {"success": True, "indices": indices}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/enhanced-mapping/{env_id}/{index_name}")
async def get_enhanced_mapping(env_id: int, index_name: str):
    """Get enhanced mapping with performance analysis"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        mapping = get_elasticsearch_mapping_enhanced(
            env['host_url'],
            index_name,
            env.get('username'),
            env.get('password')
        )
        return {"success": True, "mapping": mapping}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.put("/api/enhanced-settings/{env_id}/{index_name}")
async def update_enhanced_settings(env_id: int, index_name: str, settings: dict):
    """Update index settings with validation"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        result = update_elasticsearch_settings(
            env['host_url'],
            index_name,
            settings,
            env.get('username'),
            env.get('password')
        )
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/bulk-operations/{env_id}")
async def execute_bulk_operations(env_id: int, operation: dict):
    """Execute bulk operations on multiple indices"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        result = execute_elasticsearch_bulk_operation(
            env['host_url'],
            operation,
            env.get('username'),
            env.get('password')
        )
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_elasticsearch_indices_enhanced(host_url: str, username: Optional[str] = None, password: Optional[str] = None):
    """Get indices with enhanced metadata including health, settings, and performance"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        # Get basic indices info
        indices_response = requests.get(
            f"{host_url}/_cat/indices?format=json&h=index,status,health,pri,rep,docs.count,store.size,creation.date",
            auth=auth,
            timeout=10,
            verify=False
        )

        if indices_response.status_code == 200:
            indices_data = indices_response.json()
            enhanced_indices = []

            for index_info in indices_data:
                # Get additional metadata
                settings_response = requests.get(
                    f"{host_url}/{index_info['index']}/_settings",
                    auth=auth,
                    timeout=10,
                    verify=False
                )

                enhanced_index = {
                    'name': index_info['index'],
                    'status': index_info['status'],
                    'health': index_info['health'],
                    'primary': int(index_info['pri']),
                    'replica': int(index_info['rep']),
                    'docs': int(index_info['docs.count'] or 0),
                    'size': index_info['store.size'],
                    'created': index_info.get('creation.date'),
                    'settings': settings_response.json() if settings_response.status_code == 200 else {}
                }

                enhanced_indices.append(enhanced_index)

            return enhanced_indices
        else:
            raise Exception(f"Failed to fetch indices: {indices_response.status_code}")

    except Exception as e:
        raise Exception(f"Error fetching enhanced indices: {str(e)}")





def get_elasticsearch_mapping_enhanced(host_url: str, index_name: str, username: Optional[str] = None, password: Optional[str] = None):
    """Get mapping with performance analysis"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        # Get mapping
        mapping_response = requests.get(
            f"{host_url}/{index_name}/_mapping",
            auth=auth,
            timeout=10,
            verify=False
        )

        # Get stats
        stats_response = requests.get(
            f"{host_url}/{index_name}/_stats",
            auth=auth,
            timeout=10,
            verify=False
        )

        if mapping_response.status_code == 200:
            mapping_data = mapping_response.json()
            stats_data = stats_response.json() if stats_response.status_code == 200 else {}

            return {
                'mapping': mapping_data,
                'stats': stats_data,
                'analysis': analyze_mapping_performance(mapping_data, stats_data)
            }
        else:
            raise Exception(f"Failed to fetch mapping: {mapping_response.status_code}")

    except Exception as e:
        raise Exception(f"Error fetching enhanced mapping: {str(e)}")

def analyze_mapping_performance(mapping_data: dict, stats_data: dict) -> dict:
    """Analyze mapping for performance implications"""
    analysis = {
        'field_count': 0,
        'text_fields': 0,
        'keyword_fields': 0,
        'nested_fields': 0,
        'memory_estimate': 'Medium',
        'search_performance': 'Good',
        'recommendations': []
    }

    def count_fields(properties: dict):
        for field_name, field_config in properties.items():
            analysis['field_count'] += 1

            field_type = field_config.get('type', 'text')
            if field_type == 'text':
                analysis['text_fields'] += 1
            elif field_type == 'keyword':
                analysis['keyword_fields'] += 1
            elif field_type == 'nested':
                analysis['nested_fields'] += 1

            # Recursive for nested objects
            if 'properties' in field_config:
                count_fields(field_config['properties'])

    # Analyze mapping structure
    for index_name, index_data in mapping_data.items():
        if 'mappings' in index_data and 'properties' in index_data['mappings']:
            count_fields(index_data['mappings']['properties'])

    # Generate recommendations
    if analysis['text_fields'] > 50:
        analysis['recommendations'].append("Consider using keyword type for non-analyzed fields")

    if analysis['nested_fields'] > 10:
        analysis['recommendations'].append("Monitor nested field performance")

    if analysis['field_count'] > 1000:
        analysis['recommendations'].append("Consider field mapping optimization")

    return analysis




def update_elasticsearch_settings(
        host_url: str,
        index_name: str,
        settings: Dict[str, Any],
        username: Optional[str] = None,
        password: Optional[str] = None
) -> Dict[str, Any]:
    """Update Elasticsearch index settings with validation"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        # Validate settings before applying
        validation_result = validate_index_settings(settings)
        if not validation_result['valid']:
            return {
                "success": False,
                "error": f"Invalid settings: {validation_result['errors']}"
            }

        # Separate dynamic and static settings
        dynamic_settings = {}
        static_settings = {}

        for key, value in settings.items():
            if is_dynamic_setting(key):
                dynamic_settings[key] = value
            else:
                static_settings[key] = value

        results = []

        # Apply dynamic settings (can be updated on open index)
        if dynamic_settings:
            dynamic_payload = {"index": dynamic_settings}

            response = requests.put(
                f"{host_url}/{index_name}/_settings",
                json=dynamic_payload,
                auth=auth,
                timeout=30,
                verify=False
            )

            if response.status_code == 200:
                results.append({
                    "type": "dynamic",
                    "success": True,
                    "settings": dynamic_settings,
                    "message": "Dynamic settings updated successfully"
                })
            else:
                results.append({
                    "type": "dynamic",
                    "success": False,
                    "error": f"Failed to update dynamic settings: {response.text}",
                    "status_code": response.status_code
                })

        # Handle static settings (require index to be closed)
        if static_settings:
            static_result = update_static_settings(
                host_url, index_name, static_settings, auth
            )
            results.append(static_result)

        # Determine overall success
        overall_success = all(result.get('success', False) for result in results)

        return {
            "success": overall_success,
            "results": results,
            "timestamp": datetime.now().isoformat(),
            "index": index_name
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"Error updating settings: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }

def update_static_settings(
        host_url: str,
        index_name: str,
        static_settings: Dict[str, Any],
        auth: Optional[tuple]
) -> Dict[str, Any]:
    """Update static settings that require index to be closed"""
    try:
        # Step 1: Close the index
        close_response = requests.post(
            f"{host_url}/{index_name}/_close",
            auth=auth,
            timeout=30,
            verify=False
        )

        if close_response.status_code != 200:
            return {
                "type": "static",
                "success": False,
                "error": f"Failed to close index: {close_response.text}"
            }

        # Step 2: Update settings
        static_payload = {"index": static_settings}
        settings_response = requests.put(
            f"{host_url}/{index_name}/_settings",
            json=static_payload,
            auth=auth,
            timeout=30,
            verify=False
        )

        # Step 3: Reopen the index
        open_response = requests.post(
            f"{host_url}/{index_name}/_open",
            auth=auth,
            timeout=30,
            verify=False
        )

        if settings_response.status_code == 200 and open_response.status_code == 200:
            return {
                "type": "static",
                "success": True,
                "settings": static_settings,
                "message": "Static settings updated successfully (index was closed and reopened)"
            }
        else:
            return {
                "type": "static",
                "success": False,
                "error": f"Settings update failed - Settings: {settings_response.status_code}, Open: {open_response.status_code}",
                "settings_error": settings_response.text if settings_response.status_code != 200 else None,
                "open_error": open_response.text if open_response.status_code != 200 else None
            }

    except Exception as e:
        return {
            "type": "static",
            "success": False,
            "error": f"Error updating static settings: {str(e)}"
        }

def is_dynamic_setting(setting_key: str) -> bool:
    """Check if a setting can be updated dynamically (without closing the index)"""
    dynamic_settings = {
        'number_of_replicas',
        'refresh_interval',
        'max_result_window',
        'max_inner_result_window',
        'max_rescore_window',
        'max_docvalue_fields_search',
        'max_script_fields',
        'max_ngram_diff',
        'max_shingle_diff',
        'blocks.read_only',
        'blocks.read_only_allow_delete',
        'blocks.read',
        'blocks.write',
        'blocks.metadata',
        'max_refresh_listeners',
        'analyze.max_token_count',
        'highlight.max_analyzed_offset',
        'max_terms_count',
        'max_regex_length',
        'routing.allocation.enable',
        'routing.rebalance.enable',
        'gc_deletes',
    }

    return setting_key in dynamic_settings

def validate_index_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Validate index settings before applying"""
    errors = []
    warnings = []

    for key, value in settings.items():
        # Validate number_of_replicas
        if key == 'number_of_replicas':
            if not isinstance(value, int) or value < 0:
                errors.append(f"number_of_replicas must be a non-negative integer, got: {value}")

        # Validate refresh_interval
        elif key == 'refresh_interval':
            if value != -1 and not isinstance(value, str):
                errors.append(f"refresh_interval must be a time string (e.g., '1s') or -1, got: {value}")

        # Validate max_result_window
        elif key == 'max_result_window':
            if not isinstance(value, int) or value <= 0:
                errors.append(f"max_result_window must be a positive integer, got: {value}")
            elif value > 2147483647:
                errors.append(f"max_result_window too large: {value}")

        # Validate number_of_shards (static setting)
        elif key == 'number_of_shards':
            if not isinstance(value, int) or value <= 0:
                errors.append(f"number_of_shards must be a positive integer, got: {value}")
            elif value > 1024:
                warnings.append(f"number_of_shards is very high: {value}. Consider if this is necessary.")

        # Validate codec
        elif key == 'codec':
            valid_codecs = ['default', 'best_compression', 'lucene_default']
            if value not in valid_codecs:
                errors.append(f"codec must be one of {valid_codecs}, got: {value}")

    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }

def execute_elasticsearch_bulk_operation(
        host_url: str,
        operation: Dict[str, Any],
        username: Optional[str] = None,
        password: Optional[str] = None
) -> Dict[str, Any]:
    """Execute bulk operations on multiple Elasticsearch indices"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        operation_type = operation.get('type')
        indices = operation.get('indices', [])
        parameters = operation.get('parameters', {})

        if not indices:
            return {
                "success": False,
                "error": "No indices specified for bulk operation"
            }

        results = []

        # Execute operation based on type
        if operation_type == 'update_settings':
            results = bulk_update_settings(host_url, indices, parameters, auth)

        elif operation_type == 'create_aliases':
            results = bulk_create_aliases(host_url, indices, parameters, auth)

        elif operation_type == 'delete_aliases':
            results = bulk_delete_aliases(host_url, indices, parameters, auth)

        elif operation_type == 'reindex':
            results = bulk_reindex(host_url, indices, parameters, auth)

        elif operation_type == 'close':
            results = bulk_close_indices(host_url, indices, auth)

        elif operation_type == 'open':
            results = bulk_open_indices(host_url, indices, auth)

        elif operation_type == 'delete':
            results = bulk_delete_indices(host_url, indices, auth)

        elif operation_type == 'force_merge':
            results = bulk_force_merge(host_url, indices, parameters, auth)

        elif operation_type == 'refresh':
            results = bulk_refresh_indices(host_url, indices, auth)

        elif operation_type == 'flush':
            results = bulk_flush_indices(host_url, indices, auth)

        else:
            return {
                "success": False,
                "error": f"Unknown bulk operation type: {operation_type}"
            }

        # Calculate overall success rate
        successful_operations = sum(1 for result in results if result.get('success', False))
        total_operations = len(results)
        success_rate = successful_operations / total_operations if total_operations > 0 else 0

        return {
            "success": success_rate > 0.5,  # Consider successful if more than 50% succeed
            "operation_type": operation_type,
            "total_indices": len(indices),
            "successful_operations": successful_operations,
            "failed_operations": total_operations - successful_operations,
            "success_rate": round(success_rate * 100, 2),
            "results": results,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"Error executing bulk operation: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }

def bulk_update_settings(
        host_url: str,
        indices: List[str],
        settings: Dict[str, Any],
        auth: Optional[tuple]
) -> List[Dict[str, Any]]:
    """Update settings for multiple indices"""
    results = []

    for index_name in indices:
        try:
            result = update_elasticsearch_settings(
                host_url, index_name, settings,
                auth[0] if auth else None,
                auth[1] if auth else None
            )
            results.append({
                "index": index_name,
                "operation": "update_settings",
                "success": result.get('success', False),
                "details": result
            })
        except Exception as e:
            results.append({
                "index": index_name,
                "operation": "update_settings",
                "success": False,
                "error": str(e)
            })

    return results

def bulk_create_aliases(
        host_url: str,
        indices: List[str],
        parameters: Dict[str, Any],
        auth: Optional[tuple]
) -> List[Dict[str, Any]]:
    """Create aliases for multiple indices"""
    results = []
    alias_name = parameters.get('alias_name')

    if not alias_name:
        return [{
            "operation": "create_aliases",
            "success": False,
            "error": "alias_name parameter is required"
        }]

    # Build bulk alias request
    actions = []
    for index_name in indices:
        actions.append({
            "add": {
                "index": index_name,
                "alias": alias_name
            }
        })

    try:
        response = requests.post(
            f"{host_url}/_aliases",
            json={"actions": actions},
            auth=auth,
            timeout=30,
            verify=False
        )

        if response.status_code == 200:
            for index_name in indices:
                results.append({
                    "index": index_name,
                    "operation": "create_alias",
                    "success": True,
                    "alias": alias_name
                })
        else:
            for index_name in indices:
                results.append({
                    "index": index_name,
                    "operation": "create_alias",
                    "success": False,
                    "error": response.text
                })

    except Exception as e:
        for index_name in indices:
            results.append({
                "index": index_name,
                "operation": "create_alias",
                "success": False,
                "error": str(e)
            })

    return results

def bulk_delete_aliases(
        host_url: str,
        indices: List[str],
        parameters: Dict[str, Any],
        auth: Optional[tuple]
) -> List[Dict[str, Any]]:
    """Delete aliases from multiple indices"""
    results = []
    alias_name = parameters.get('alias_name')

    if not alias_name:
        return [{
            "operation": "delete_aliases",
            "success": False,
            "error": "alias_name parameter is required"
        }]

    # Build bulk alias delete request
    actions = []
    for index_name in indices:
        actions.append({
            "remove": {
                "index": index_name,
                "alias": alias_name
            }
        })

    try:
        response = requests.post(
            f"{host_url}/_aliases",
            json={"actions": actions},
            auth=auth,
            timeout=30,
            verify=False
        )

        if response.status_code == 200:
            for index_name in indices:
                results.append({
                    "index": index_name,
                    "operation": "delete_alias",
                    "success": True,
                    "alias": alias_name
                })
        else:
            for index_name in indices:
                results.append({
                    "index": index_name,
                    "operation": "delete_alias",
                    "success": False,
                    "error": response.text
                })

    except Exception as e:
        for index_name in indices:
            results.append({
                "index": index_name,
                "operation": "delete_alias",
                "success": False,
                "error": str(e)
            })

    return results

def bulk_reindex(
        host_url: str,
        indices: List[str],
        parameters: Dict[str, Any],
        auth: Optional[tuple]
) -> List[Dict[str, Any]]:
    """Reindex multiple indices"""
    results = []
    dest_suffix = parameters.get('dest_suffix', '_reindexed')

    for index_name in indices:
        try:
            dest_index = f"{index_name}{dest_suffix}"

            reindex_body = {
                "source": {"index": index_name},
                "dest": {"index": dest_index}
            }

            # Add any additional reindex parameters
            if 'query' in parameters:
                reindex_body['source']['query'] = parameters['query']

            if 'script' in parameters:
                reindex_body['script'] = parameters['script']

            response = requests.post(
                f"{host_url}/_reindex",
                json=reindex_body,
                auth=auth,
                timeout=300,  # Longer timeout for reindex
                verify=False
            )

            if response.status_code == 200:
                reindex_result = response.json()
                results.append({
                    "index": index_name,
                    "operation": "reindex",
                    "success": True,
                    "destination": dest_index,
                    "documents_processed": reindex_result.get('total', 0),
                    "time_taken": reindex_result.get('took', 0)
                })
            else:
                results.append({
                    "index": index_name,
                    "operation": "reindex",
                    "success": False,
                    "error": response.text
                })

        except Exception as e:
            results.append({
                "index": index_name,
                "operation": "reindex",
                "success": False,
                "error": str(e)
            })

    return results

def bulk_close_indices(host_url: str, indices: List[str], auth: Optional[tuple]) -> List[Dict[str, Any]]:
    """Close multiple indices"""
    return bulk_index_operation(host_url, indices, "close", auth)

def bulk_open_indices(host_url: str, indices: List[str], auth: Optional[tuple]) -> List[Dict[str, Any]]:
    """Open multiple indices"""
    return bulk_index_operation(host_url, indices, "open", auth)

def bulk_delete_indices(host_url: str, indices: List[str], auth: Optional[tuple]) -> List[Dict[str, Any]]:
    """Delete multiple indices"""
    results = []

    for index_name in indices:
        try:
            response = requests.delete(
                f"{host_url}/{index_name}",
                auth=auth,
                timeout=30,
                verify=False
            )

            results.append({
                "index": index_name,
                "operation": "delete",
                "success": response.status_code == 200,
                "error": response.text if response.status_code != 200 else None
            })

        except Exception as e:
            results.append({
                "index": index_name,
                "operation": "delete",
                "success": False,
                "error": str(e)
            })

    return results

def bulk_force_merge(
        host_url: str,
        indices: List[str],
        parameters: Dict[str, Any],
        auth: Optional[tuple]
) -> List[Dict[str, Any]]:
    """Force merge multiple indices"""
    results = []
    max_num_segments = parameters.get('max_num_segments', 1)

    for index_name in indices:
        try:
            response = requests.post(
                f"{host_url}/{index_name}/_forcemerge?max_num_segments={max_num_segments}",
                auth=auth,
                timeout=300,  # Longer timeout for force merge
                verify=False
            )

            if response.status_code == 200:
                merge_result = response.json()
                results.append({
                    "index": index_name,
                    "operation": "force_merge",
                    "success": True,
                    "segments_merged": merge_result.get('_shards', {}).get('successful', 0)
                })
            else:
                results.append({
                    "index": index_name,
                    "operation": "force_merge",
                    "success": False,
                    "error": response.text
                })

        except Exception as e:
            results.append({
                "index": index_name,
                "operation": "force_merge",
                "success": False,
                "error": str(e)
            })

    return results

def bulk_refresh_indices(host_url: str, indices: List[str], auth: Optional[tuple]) -> List[Dict[str, Any]]:
    """Refresh multiple indices"""
    return bulk_index_operation(host_url, indices, "refresh", auth)

def bulk_flush_indices(host_url: str, indices: List[str], auth: Optional[tuple]) -> List[Dict[str, Any]]:
    """Flush multiple indices"""
    return bulk_index_operation(host_url, indices, "flush", auth)

def bulk_index_operation(
        host_url: str,
        indices: List[str],
        operation: str,
        auth: Optional[tuple]
) -> List[Dict[str, Any]]:
    """Generic bulk operation for simple index operations"""
    results = []

    for index_name in indices:
        try:
            if operation in ['close', 'open']:
                response = requests.post(
                    f"{host_url}/{index_name}/_{operation}",
                    auth=auth,
                    timeout=30,
                    verify=False
                )
            else:  # refresh, flush
                response = requests.post(
                    f"{host_url}/{index_name}/_{operation}",
                    auth=auth,
                    timeout=30,
                    verify=False
                )

            results.append({
                "index": index_name,
                "operation": operation,
                "success": response.status_code == 200,
                "error": response.text if response.status_code != 200 else None
            })

        except Exception as e:
            results.append({
                "index": index_name,
                "operation": operation,
                "success": False,
                "error": str(e)
            })

    return results

# Additional utility functions for the Enhanced Indices Manager

def get_cluster_health(host_url: str, username: Optional[str] = None, password: Optional[str] = None) -> Dict[str, Any]:
    """Get comprehensive cluster health information"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        response = requests.get(
            f"{host_url}/_cluster/health",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            health_data = response.json()

            # Get additional cluster stats
            stats_response = requests.get(
                f"{host_url}/_cluster/stats",
                auth=auth,
                timeout=10,
                verify=False
            )

            stats_data = stats_response.json() if stats_response.status_code == 200 else {}

            return {
                "success": True,
                "health": health_data,
                "stats": stats_data,
                "enhanced_metrics": {
                    "health_score": calculate_health_score(health_data),
                    "performance_rating": calculate_performance_rating(health_data, stats_data),
                    "recommendations": generate_cluster_recommendations(health_data, stats_data)
                }
            }
        else:
            return {
                "success": False,
                "error": f"Failed to get cluster health: {response.status_code}"
            }

    except Exception as e:
        return {
            "success": False,
            "error": f"Error getting cluster health: {str(e)}"
        }

def calculate_health_score(health_data: Dict[str, Any]) -> int:
    """Calculate a health score from 0-100 based on cluster health"""
    base_score = {
        'green': 100,
        'yellow': 70,
        'red': 30
    }.get(health_data.get('status', 'red'), 30)

    # Adjust based on other factors
    if health_data.get('unassigned_shards', 0) > 0:
        base_score -= min(health_data['unassigned_shards'] * 5, 30)

    if health_data.get('delayed_unassigned_shards', 0) > 0:
        base_score -= min(health_data['delayed_unassigned_shards'] * 3, 20)

    return max(0, min(100, base_score))

def calculate_performance_rating(health_data: Dict[str, Any], stats_data: Dict[str, Any]) -> str:
    """Calculate performance rating based on cluster metrics"""
    health_score = calculate_health_score(health_data)

    if health_score >= 90:
        return "Excellent"
    elif health_score >= 75:
        return "Good"
    elif health_score >= 50:
        return "Fair"
    else:
        return "Poor"

def generate_cluster_recommendations(health_data: Dict[str, Any], stats_data: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on cluster health and stats"""
    recommendations = []

    if health_data.get('status') == 'yellow':
        recommendations.append("Cluster is in yellow state - check replica shard allocation")

    if health_data.get('status') == 'red':
        recommendations.append("CRITICAL: Cluster is in red state - immediate attention required")

    if health_data.get('unassigned_shards', 0) > 0:
        recommendations.append(f"Fix {health_data['unassigned_shards']} unassigned shards")

    if health_data.get('number_of_pending_tasks', 0) > 10:
        recommendations.append("High number of pending tasks - check cluster performance")

    # Add more recommendations based on stats_data if available
    if stats_data and 'nodes' in stats_data:
        node_count = stats_data['nodes'].get('count', {}).get('total', 0)
        if node_count < 3:
            recommendations.append("Consider adding more nodes for better resilience")

    return recommendations

# Add these new API endpoints to your main FastAPI application

@app.get("/api/cluster-health/{env_id}")
async def get_cluster_health_endpoint(env_id: int):
    """Get enhanced cluster health information"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        health_data = get_cluster_health(
            env['host_url'],
            env.get('username'),
            env.get('password')
        )
        return health_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/enhanced-indices-with-performance/{env_id}")
async def get_enhanced_indices_with_performance(env_id: int):
    """Get indices with performance metrics"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Get basic indices
        indices = get_elasticsearch_indices_enhanced(
            env['host_url'],
            env.get('username'),
            env.get('password')
        )

        # Add performance metrics for each index
        for index in indices:
            try:
                perf_metrics = get_index_performance_metrics(
                    env['host_url'],
                    index['name'],
                    env.get('username'),
                    env.get('password')
                )
                index['performance'] = perf_metrics
            except Exception as e:
                index['performance'] = {
                    'error': str(e),
                    'searchLatency': 0,
                    'indexingRate': 0,
                    'cacheHitRatio': 0,
                    'memoryUsage': 'Unknown'
                }

        return {"success": True, "indices": indices}
    except Exception as e:
        return {"success": False, "error": str(e)}

def get_index_performance_metrics(
        host_url: str,
        index_name: str,
        username: Optional[str] = None,
        password: Optional[str] = None
) -> Dict[str, Any]:
    """Get performance metrics for a specific index"""
    try:
        if not host_url.startswith(('http://', 'https://')):
            host_url = f'http://{host_url}'

        auth = None
        if username and password:
            auth = (username, password)

        # Get index stats
        response = requests.get(
            f"{host_url}/{index_name}/_stats",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            stats = response.json()
            index_stats = stats.get('indices', {}).get(index_name, {})

            # Extract performance metrics
            total_stats = index_stats.get('total', {})
            search_stats = total_stats.get('search', {})
            indexing_stats = total_stats.get('indexing', {})

            # Calculate metrics
            search_time = search_stats.get('query_time_in_millis', 0)
            search_count = search_stats.get('query_total', 1)
            avg_search_latency = search_time / search_count if search_count > 0 else 0

            indexing_time = indexing_stats.get('index_time_in_millis', 0)
            indexing_count = indexing_stats.get('index_total', 1)
            avg_indexing_rate = (indexing_count / (indexing_time / 1000)) if indexing_time > 0 else 0

            return {
                "searchLatency": round(avg_search_latency, 2),
                "indexingRate": round(avg_indexing_rate, 2),
                "cacheHitRatio": 0.85,  # This would need to be calculated from cache stats
                "memoryUsage": f"{total_stats.get('store', {}).get('size_in_bytes', 0) // (1024*1024)}MB"
            }
        else:
            raise Exception(f"Failed to get stats: {response.status_code}")

    except Exception as e:
        raise Exception(f"Error getting performance metrics: {str(e)}")


@app.get("/api/enhanced-settings/{env_id}/{index_name}")
async def get_enhanced_settings(env_id: int, index_name: str):
    """Get enhanced settings for a specific index"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Get index settings from Elasticsearch
        if not env['host_url'].startswith(('http://', 'https://')):
            env['host_url'] = f"http://{env['host_url']}"

        auth = None
        if env.get('username') and env.get('password'):
            auth = (env['username'], env['password'])

        response = requests.get(
            f"{env['host_url']}/{index_name}/_settings",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            settings_data = response.json()
            index_settings = settings_data.get(index_name, {}).get('settings', {})

            return {
                "success": True,
                "settings": index_settings,
                "index_name": index_name
            }
        elif response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")
        else:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to get settings: {response.text}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting settings: {str(e)}")

@app.get("/api/performance-metrics/{env_id}/{index_name}")
async def get_performance_metrics(env_id: int, index_name: str):
    """Get performance metrics for a specific index"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Get index stats from Elasticsearch
        if not env['host_url'].startswith(('http://', 'https://')):
            env['host_url'] = f"http://{env['host_url']}"

        auth = None
        if env.get('username') and env.get('password'):
            auth = (env['username'], env['password'])

        # Get index stats
        stats_response = requests.get(
            f"{env['host_url']}/{index_name}/_stats",
            auth=auth,
            timeout=10,
            verify=False
        )

        if stats_response.status_code == 200:
            stats_data = stats_response.json()
            index_stats = stats_data.get('indices', {}).get(index_name, {})

            # Extract performance metrics
            total_stats = index_stats.get('total', {})
            search_stats = total_stats.get('search', {})
            indexing_stats = total_stats.get('indexing', {})
            store_stats = total_stats.get('store', {})

            # Calculate metrics
            search_time = search_stats.get('query_time_in_millis', 0)
            search_count = search_stats.get('query_total', 1)
            avg_search_latency = search_time / search_count if search_count > 0 else 0

            indexing_time = indexing_stats.get('index_time_in_millis', 0)
            indexing_count = indexing_stats.get('index_total', 1)
            avg_indexing_rate = (indexing_count / (indexing_time / 1000)) if indexing_time > 0 else 0

            # Get cache stats if available
            query_cache = total_stats.get('query_cache', {})
            cache_hits = query_cache.get('hit_count', 0)
            cache_total = cache_hits + query_cache.get('miss_count', 0)
            cache_hit_ratio = cache_hits / cache_total if cache_total > 0 else 0

            performance_metrics = {
                "searchLatency": round(avg_search_latency, 2),
                "indexingRate": round(avg_indexing_rate, 2),
                "cacheHitRatio": round(cache_hit_ratio, 3),
                "memoryUsage": f"{store_stats.get('size_in_bytes', 0) // (1024*1024)}MB",
                "searchTotal": search_stats.get('query_total', 0),
                "indexingTotal": indexing_stats.get('index_total', 0),
                "searchTime": search_stats.get('query_time_in_millis', 0),
                "indexingTime": indexing_stats.get('index_time_in_millis', 0),
                "storeSize": store_stats.get('size_in_bytes', 0),
                "documentCount": total_stats.get('docs', {}).get('count', 0)
            }

            return {
                "success": True,
                "performance": performance_metrics,
                "index_name": index_name,
                "timestamp": datetime.now().isoformat()
            }
        elif stats_response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")
        else:
            raise HTTPException(status_code=stats_response.status_code, detail=f"Failed to get stats: {stats_response.text}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting performance metrics: {str(e)}")

@app.get("/api/index-aliases/{env_id}/{index_name}")
async def get_index_aliases(env_id: int, index_name: str):
    """Get aliases for a specific index"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Get index aliases from Elasticsearch
        if not env['host_url'].startswith(('http://', 'https://')):
            env['host_url'] = f"http://{env['host_url']}"

        auth = None
        if env.get('username') and env.get('password'):
            auth = (env['username'], env['password'])

        response = requests.get(
            f"{env['host_url']}/{index_name}/_alias",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            aliases_data = response.json()
            index_aliases = aliases_data.get(index_name, {}).get('aliases', {})
            aliases_list = list(index_aliases.keys())

            return {
                "success": True,
                "aliases": aliases_list,
                "index_name": index_name,
                "total_aliases": len(aliases_list)
            }
        elif response.status_code == 404:
            # Index exists but has no aliases
            return {
                "success": True,
                "aliases": [],
                "index_name": index_name,
                "total_aliases": 0
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to get aliases: {response.text}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting aliases: {str(e)}")


@app.get("/api/index-health/{env_id}/{index_name}")
async def get_index_health(env_id: int, index_name: str):
    """Get detailed health information for a specific index"""
    try:
        environments = get_elasticsearch_environments()
        env = next((e for e in environments if e['id'] == env_id), None)
        if not env:
            raise HTTPException(status_code=404, detail="Environment not found")

        # Get index health from Elasticsearch
        if not env['host_url'].startswith(('http://', 'https://')):
            env['host_url'] = f"http://{env['host_url']}"

        auth = None
        if env.get('username') and env.get('password'):
            auth = (env['username'], env['password'])

        # Get detailed index health
        response = requests.get(
            f"{env['host_url']}_cluster/health/{index_name}?level=shards",
            auth=auth,
            timeout=10,
            verify=False
        )

        if response.status_code == 200:
            health_data = response.json()

            return {
                "success": True,
                "health": health_data,
                "index_name": index_name,
                "status": health_data.get('status', 'unknown'),
                "active_shards": health_data.get('active_shards', 0),
                "relocating_shards": health_data.get('relocating_shards', 0),
                "initializing_shards": health_data.get('initializing_shards', 0),
                "unassigned_shards": health_data.get('unassigned_shards', 0)
            }
        else:
            # Fallback to basic health info
            return {
                "success": True,
                "health": {"status": "yellow"},
                "index_name": index_name,
                "status": "yellow",
                "active_shards": 1,
                "relocating_shards": 0,
                "initializing_shards": 0,
                "unassigned_shards": 0
            }

    except Exception as e:
        # Return default health info on error
        return {
            "success": True,
            "health": {"status": "yellow"},
            "index_name": index_name,
            "status": "yellow",
            "active_shards": 1,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
            "error": str(e)
        }


def add_prefix_to_keys(data: dict, keys: list, prefix_source: list) -> dict:
    """
    Add prefix (from prefix_source) to keys present in both data and keys list.

    Args:
        data (dict): Original dictionary
        keys (list): Keys to check for prefixing
        prefix_source (list): List with 'something:prefix' format

    Returns:
        dict: New dictionary with prefixed keys where applicable
    """
    if not prefix_source or ":" not in prefix_source[0]:
        raise ValueError("prefix_source must contain at least one item with ':'")

    prefix = prefix_source[0].split(":")[1]

    new_dict = {}
    for k, v in data.items():
        if k in keys:
            new_dict[f"{prefix}#{k}"] = v
        else:
            new_dict[k] = v

    return new_dict

def build_query_v6(
        result: dict,
        operator: str,
        index: str,
        nested_field_list: list | set | None = None,
        inner_field_list: list | set | None = None
):
    """
    Build a structured query with support for:
      - base (root) conditions (main_groups)
      - nested field groups (nested_groups)
      - inner (parent-child or inner_hits style) groups (inner_groups)
      - multi_value fields with AND / OR semantics

    Arguments
    ---------
    result : dict
        Field -> value map. Supports:
          {"field": value}  # single value
          {"field": {"type":"multi_value","operator":"AND|OR","values":[...]}}
    operator : str
        Main boolean operator for base conditions (e.g. "AND" or "OR").
    index : str
        Index name.
    nested_field_list : list|set|None
        Optional list or set of field *roots* that should be treated as nested.
        Example: {"addresses", "orders.items"}
        (We classify by the first path segment of the field.)
    inner_field_list : list|set|None
        Optional list or set of field *roots* that should be treated as inner.
        (Used for parent-child / inner_hits style grouping if your pipeline supports it.)
    """

    # ---- helpers -------------------------------------------------------------

    def _root(field: str) -> str:
        # first path segment is used as the "container" root
        return field.split(".")[0]

    def _is_nested(field_root: str) -> bool:
        # Treat as nested if listed OR the field is dot-notated by default
        return (
                (field_root in nested_field_list_norm)
                or ("." in field_root)  # very defensive (rare)
        )

    def _is_inner(field_root: str) -> bool:
        return (
                (field_root in inner_field_list_norm)
                or ("#" in field_root)  # very defensive (rare)
        )

    def _add_group(groups: list, key: str, cond: dict, group_operator: str, kind: str):

        """
        Add condition to an existing group with the same (kind, key, operator),
        else create a new one. For `nested_groups`, key is the nested_path;
        for `inner_groups`, key is the inner_path.
        """
        path_key = "nested_path" if kind == "nested" else "has_child_type"
        for g in groups:
            if g.get(path_key) == key and g.get("operator") == group_operator:
                g["conditions"].append(cond)
                return
        if path_key !="has_child_type":
            groups.append({
                path_key: key,
                "operator": group_operator,
                "conditions": [cond],
            })
        print()
        if path_key =="has_child_type":
            groups.append({
                path_key: key.split("#")[0],
                "operator": group_operator,
                "conditions": [cond],
            })

    # Normalize lists to sets for fast membership checks
    nested_field_list_norm = set(nested_field_list or [])
    inner_field_list_norm = set(inner_field_list or [])

    main_groups = []
    base_conditions = []

    nested_groups = []
    inner_groups = []

    # Keep OR groups per scope so multiple ORs on different fields don’t overwrite each other
    base_or_groups_by_id = {}   # id -> {"id", "operator":"OR","conditions":[]}

    for key, value in result.items():
        original_key = key  # keep for classification before we touch suffixes
        field_root = _root(original_key)

        # classification order: inner > nested > base
        is_inner = _is_inner(field_root)
        is_nested = (not is_inner) and (_is_nested(field_root) or ("." in original_key))
        print(is_inner)
        # ---- MULTI-VALUE -----------------------------------------------------
        if isinstance(value, dict) and value.get("type") == "multi_value":
            doc_type_operator = value.get("operator", "AND")
            values = value.get("values", [])

            # For text-ish exact matching across multiple values, default to keyword sub-field
            mv_key = f"{original_key}.keyword"

            if doc_type_operator == "AND":
                # AND => each value becomes its own condition
                for v in values:
                    cond = create_condition(mv_key, "match", v, is_nested or is_inner)
                    if is_inner:
                        _add_group(inner_groups, field_root, cond, "AND", kind="inner")
                    elif is_nested:
                        _add_group(nested_groups, field_root, cond, "AND", kind="nested")
                    else:
                        base_conditions.append(cond)

            elif doc_type_operator == "OR":
                # OR => one "in" condition holding list of values
                cond = create_condition(mv_key, "in", values, is_nested or is_inner)
                if is_inner:
                    _add_group(inner_groups, field_root, cond, "OR", kind="inner")
                elif is_nested:
                    _add_group(nested_groups, field_root, cond, "OR", kind="nested")
                else:
                    group_id = f"{original_key}_group"
                    grp = base_or_groups_by_id.get(group_id)
                    if not grp:
                        grp = {"id": group_id, "operator": "OR", "conditions": []}
                        base_or_groups_by_id[group_id] = grp
                    grp["conditions"].append(cond)

            else:
                # Fallback: treat unknown multi_value operator as AND
                print(is_inner)
                for v in values:
                    cond = create_condition(mv_key, "match", v, is_nested or is_inner)
                    if is_inner:
                        _add_group(inner_groups, field_root, cond, "AND", kind="inner")
                    elif is_nested:
                        _add_group(nested_groups, field_root, cond, "AND", kind="nested")
                    else:
                        base_conditions.append(cond)

        # ---- SINGLE-VALUE ----------------------------------------------------
        else:
            op = resolve_operator_v1(value)
            cond = create_condition(original_key, op, value, is_nested or is_inner)
            if is_inner:
                _add_group(inner_groups, field_root, cond, "AND", kind="inner")
            elif is_nested:
                _add_group(nested_groups, field_root, cond, "AND", kind="nested")
            else:
                base_conditions.append(cond)

    # Base main group (root conditions)
    if base_conditions:
        main_groups.append({
            "id": "mainGroup",
            "operator": operator,
            "conditions": base_conditions
        })

    # Any base-level OR groups collected
    for grp in base_or_groups_by_id.values():
        if grp["conditions"]:
            main_groups.append(grp)

    return {
        "index": index,
        "main_groups": main_groups,
        "nested_groups": nested_groups,
        "inner_groups": inner_groups,
    }

def transform_query_v6(input_data):
    """
    Transform query data (from build_query_v5) into the final query structure.

    Args:
        input_data (dict): {
            "index": str,
            "main_groups": [
                {"operator": "AND|OR", "conditions": [...]},
                ...
            ],
            "nested_groups": [
                {"nested_path": "...", "operator": "AND|OR", "conditions": [...]},
                ...
            ],
            "inner_groups": [
                {"inner_path": "...", "operator": "AND|OR", "conditions": [...]},
                ...
            ],
            ...
        }

    Returns:
        dict: {
          "index_name": str,
          "query": { "operator": "AND", "groups": [...] },
          "pagination": { "from": 0, "size": 10 },
          "sort": [ { "order": "desc" } ]
        }
    """
    if not input_data or "index" not in input_data:
        raise ValueError("Input data must contain 'index' field")

    # Start the output with index name
    output = {
        "index_name": input_data["index"],
        "query": {
            "operator": "AND",
            "groups": []
        },
        "pagination": {
            "from": 0,
            "size": 10
        },
        "sort": [
            {
                "order": "desc"
            }
        ]
    }

    main_groups = input_data.get("main_groups", [])
    nested_groups = input_data.get("nested_groups", [])
    inner_groups = input_data.get("inner_groups", [])

    # --- Handle main groups ---------------------------------------------------
    if len(main_groups) == 0:
        # No main groups - nothing to add here
        pass
    elif len(main_groups) == 1:
        # Simple structure: just map directly
        group = {
            "operator": main_groups[0].get("operator", "AND"),
            "conditions": main_groups[0].get("conditions", [])
        }
        # In case upstream supplied nested subgroups inside a main group
        if "groups" in main_groups[0]:
            group["groups"] = main_groups[0]["groups"]
        output["query"]["groups"].append(group)
    else:
        # Multiple groups: nest additional groups inside first group
        first_group = main_groups[0]
        outer = {
            "operator": first_group.get("operator", "AND"),
            "conditions": first_group.get("conditions", []),
            "groups": []
        }

        # Add remaining groups as nested sub-groups
        for sub_group in main_groups[1:]:
            nested = {
                "operator": sub_group.get("operator", "AND"),
                "conditions": sub_group.get("conditions", [])
            }
            if "groups" in sub_group:
                nested["groups"] = sub_group["groups"]
            outer["groups"].append(nested)

        output["query"]["groups"].append(outer)

    # --- Handle standalone nested groups -------------------------------------
    # If nested group has AND with multiple conditions, split into separate groups
    if nested_groups:
        for ng in nested_groups:
            ng_operator = ng.get("operator", "AND")
            ng_path = ng.get("nested_path")
            ng_conditions = ng.get("conditions", [])

            if ng_operator == "AND" and len(ng_conditions) > 1:
                for cond in ng_conditions:
                    output["query"]["groups"].append({
                        "operator": "AND",
                        "nested_path": ng_path,
                        "conditions": [cond]
                    })
            else:
                output["query"]["groups"].append({
                    "operator": ng_operator,
                    "nested_path": ng_path,
                    "conditions": ng_conditions
                })

    # --- Handle inner groups (parent-child / inner_hits style) ---------------
    # Mirror the nested group behavior, using 'inner_path'
    if inner_groups:
        for ig in inner_groups:
            ig_operator = ig.get("operator", "AND")
            ig_path = ig.get("has_child_type")
            ig_conditions = ig.get("conditions", [])

            if ig_operator == "AND" and len(ig_conditions) > 1:
                for cond in ig_conditions:
                    output["query"]["groups"].append({
                        "operator": "AND",
                        "has_child_type": ig_path,
                        "conditions": [cond]
                    })
            else:
                output["query"]["groups"].append({
                    "operator": ig_operator,
                    "has_child_type": ig_path,
                    "conditions": ig_conditions
                })

    return output



@app.on_event("startup")
async def startup_event():
    init_db()
    print("Database initialized successfully!")
    print("Oracle to Elasticsearch Mapping Generator is ready!")
    print("Access the application at: http://localhost:8000")

if __name__ == "__main__":    uvicorn.run(app, host="0.0.0.0", port=8002)