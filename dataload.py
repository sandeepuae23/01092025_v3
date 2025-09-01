"""
dataload.py - Oracle to Elasticsearch Data Loading Module

This module provides the OracleElasticsearchMapper class for automatic mapping
and bulk loading of Oracle data into Elasticsearch with case conversion and
nested field support.
"""

import json
from typing import Dict, List, Any, Optional, Tuple, Union
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
from datetime import datetime

class OracleElasticsearchMapper:
    """
    A class to handle automatic mapping and data conversion from Oracle to Elasticsearch.
    Supports case conversion, nested field detection, and bulk operations.
    """

    def __init__(self, es_client: Elasticsearch, logger: Optional[logging.Logger] = None):
        """
        Initialize the mapper with Elasticsearch client.

        Args:
            es_client: Elasticsearch client instance
            logger: Optional logger instance for detailed logging
        """
        self.es_client = es_client
        self.logger = logger or self._setup_default_logger()
        self.column_mapping = {}
        self.es_fields = {}
        self.field_structure = {}

    def _setup_default_logger(self) -> logging.Logger:
        """Setup default logger if none provided."""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def analyze_mapping(self, oracle_columns: List[str], elastic_mapping: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze Oracle columns and Elasticsearch mapping to create field relationships.

        Args:
            oracle_columns: List of Oracle column names (typically uppercase)
            elastic_mapping: Elasticsearch index mapping dictionary

        Returns:
            Dictionary with analysis results
        """
        self.logger.info(f"Analyzing mapping for {len(oracle_columns)} Oracle columns")

        # Extract ES fields from mapping
        self.es_fields = self._extract_es_fields(elastic_mapping)
        self.logger.info(f"Found {len(self.es_fields)} Elasticsearch fields")

        # Create column mapping
        self.column_mapping = self._create_column_mapping(oracle_columns, self.es_fields)

        # Analyze field structure
        self.field_structure = self._analyze_field_structure(self.es_fields)

        mapping_stats = {
            'oracle_columns_count': len(oracle_columns),
            'es_fields_count': len(self.es_fields),
            'mapped_columns': len(self.column_mapping),
            'unmapped_columns': len(oracle_columns) - len(self.column_mapping),
            'column_mapping': self.column_mapping,
            'field_structure': self.field_structure
        }

        self.logger.info(f"Mapping analysis complete: {mapping_stats['mapped_columns']}/{mapping_stats['oracle_columns_count']} columns mapped")
        return mapping_stats

    def convert_data(self, oracle_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert Oracle data format to Elasticsearch format.

        Args:
            oracle_data: List of dictionaries containing Oracle row data

        Returns:
            List of converted documents ready for Elasticsearch
        """
        if not self.column_mapping:
            raise ValueError("Column mapping not initialized. Call analyze_mapping() first.")

        self.logger.info(f"Converting {len(oracle_data)} Oracle records")
        converted_data = []

        for i, row in enumerate(oracle_data):
            try:
                converted_row = self._convert_single_row(row)
                converted_data.append(converted_row)
            except Exception as e:
                self.logger.error(f"Error converting row {i}: {e}")
                continue

        self.logger.info(f"Successfully converted {len(converted_data)}/{len(oracle_data)} records")
        return converted_data

    def bulk_index(self, oracle_data: List[Dict[str, Any]], index_name: str,
                   doc_id_field: Optional[str] = None, chunk_size: int = 1000) -> Dict[str, Any]:
        """
        Convert Oracle data and perform bulk indexing to Elasticsearch.

        Args:
            oracle_data: List of Oracle row data
            index_name: Target Elasticsearch index name
            doc_id_field: Oracle field to use as document ID (optional)
            chunk_size: Number of documents per bulk request

        Returns:
            Dictionary with mapping results and bulk operation status
        """
        # Convert data
        converted_data = self.convert_data(oracle_data)

        if not converted_data:
            return {'success': False, 'error': 'No data to index', 'success_count': 0}

        # Prepare bulk actions
        actions = self._prepare_bulk_actions(converted_data, index_name, doc_id_field)

        # Execute bulk operation
        bulk_result = self._execute_bulk_operation(actions, chunk_size)

        return {
            'column_mapping': self.column_mapping,
            'total_records': len(oracle_data),
            'converted_records': len(converted_data),
            'bulk_result': bulk_result,
            'field_structure': self.field_structure,
            'converted_data': converted_data
        }

    def get_mapping_report(self) -> str:
        """
        Generate a detailed mapping report.

        Returns:
            Formatted string report of the current mapping
        """
        if not self.column_mapping or not self.field_structure:
            return "No mapping analysis available. Run analyze_mapping() first."

        report = []
        report.append("=== Oracle to Elasticsearch Mapping Report ===\n")

        # Column mappings
        report.append("Column Mappings:")
        for oracle_col, es_field in self.column_mapping.items():
            es_info = self.es_fields.get(es_field, {})
            report.append(f"  {oracle_col} -> {es_field} ({es_info.get('type', 'unknown')})")

        report.append(f"\nField Structure Summary:")
        report.append(f"  Root fields: {len(self.field_structure.get('root_fields', []))}")
        report.append(f"  Nested fields: {len(self.field_structure.get('nested_fields', []))}")

        # Nested fields details
        nested_fields = self.field_structure.get('nested_fields', [])
        if nested_fields:
            report.append(f"\nNested Fields:")
            for field in nested_fields:
                report.append(f"  {field['name']} -> {field['path']} ({field['type']})")

        return "\n".join(report)

    def _extract_es_fields(self, mapping: Dict[str, Any], parent_path: str = '') -> Dict[str, Dict]:
        """Extract all fields from Elasticsearch mapping recursively."""
        fields = {}

        # Handle different mapping structures
        properties = mapping.get('properties', {})
        if not properties and 'mappings' in mapping:
            properties = mapping['mappings'].get('properties', {})

        for field_name, field_config in properties.items():
            current_path = f"{parent_path}.{field_name}" if parent_path else field_name

            # Store field info
            fields[field_name.lower()] = {
                'full_path': current_path,
                'type': field_config.get('type', 'unknown'),
                'is_nested': field_config.get('type') == 'nested',
                'properties': field_config.get('properties', {})
            }

            # Handle nested objects
            if field_config.get('type') == 'nested' or 'properties' in field_config:
                nested_fields = self._extract_es_fields(field_config, current_path)
                fields.update(nested_fields)

        return fields

    def _create_column_mapping(self, oracle_columns: List[str], es_fields: Dict[str, Dict]) -> Dict[str, str]:
        """Create mapping between Oracle columns and ES fields."""
        mapping = {}
        es_field_names = set(es_fields.keys())

        for oracle_col in oracle_columns:
            lowercase_col = oracle_col.lower()

            # Direct match
            if lowercase_col in es_field_names:
                mapping[oracle_col] = lowercase_col
            else:
                # Fuzzy matching
                match_found = False
                cleaned_col = lowercase_col.replace('_', '').replace('-', '')

                for es_field in es_field_names:
                    cleaned_es = es_field.replace('_', '').replace('-', '')
                    if cleaned_col == cleaned_es:
                        mapping[oracle_col] = es_field
                        match_found = True
                        break

                if not match_found:
                    self.logger.warning(f"No ES field match found for Oracle column '{oracle_col}'")

        return mapping

    def _convert_single_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single Oracle row to Elasticsearch format."""
        converted_row = {}
        nested_objects = {}

        for oracle_col, value in row.items():
            if oracle_col not in self.column_mapping:
                continue

            es_field = self.column_mapping[oracle_col]
            es_field_info = self.es_fields.get(es_field, {})

            # Handle nested fields
            if '.' in es_field_info.get('full_path', ''):
                self._handle_nested_field(converted_row, nested_objects, es_field_info['full_path'], value)
            else:
                # Root level field
                converted_value = self._convert_field_value(value, es_field_info.get('type', 'text'))
                converted_row[es_field] = converted_value

        # Merge nested objects
        for nested_path, nested_data in nested_objects.items():
            self._set_nested_value(converted_row, nested_path, nested_data)

        return converted_row

    def _handle_nested_field(self, converted_row: Dict, nested_objects: Dict, field_path: str, value: Any):
        """Handle nested field assignment."""
        path_parts = field_path.split('.')
        if len(path_parts) > 1:
            root_field = path_parts[0]
            remaining_path = '.'.join(path_parts[1:])

            if root_field not in nested_objects:
                nested_objects[root_field] = {}

            self._set_nested_value(nested_objects[root_field], remaining_path, value)

    def _set_nested_value(self, obj: Dict, path: str, value: Any):
        """Set value in nested dictionary using dot notation path."""
        keys = path.split('.')
        current = obj

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value

    def _convert_field_value(self, value: Any, es_field_type: str) -> Any:
        """Convert Oracle value to appropriate Elasticsearch type."""
        if value is None:
            return None

        type_converters = {
            'integer': lambda x: int(x) if x is not None else None,
            'long': lambda x: int(x) if x is not None else None,
            'float': lambda x: float(x) if x is not None else None,
            'double': lambda x: float(x) if x is not None else None,
            'boolean': lambda x: bool(x) if x is not None else None,
            'date': lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x),
            'text': lambda x: str(x) if x is not None else None,
            'keyword': lambda x: str(x) if x is not None else None
        }

        converter = type_converters.get(es_field_type, lambda x: x)

        try:
            return converter(value)
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Could not convert value '{value}' to type '{es_field_type}': {e}")
            return str(value)

    def _convert_date_value(self, value: Any) -> str:
        """Convert various date formats to ISO string."""
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        elif isinstance(value, str):
            # Try to parse common date formats
            try:
                # Try ISO format first
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return dt.isoformat()
            except:
                # Return as-is if parsing fails
                return value
        else:
            return str(value)

    def _prepare_bulk_actions(self, data: List[Dict[str, Any]], index_name: str,
                              doc_id_field: Optional[str] = None) -> List[Dict[str, Any]]:
        """Prepare bulk actions for Elasticsearch."""
        actions = []

        for doc in data:
            action = {
                '_index': index_name,
                '_source': doc
            }

            # Add document ID if specified
            if doc_id_field and doc_id_field in doc:
                action['_id'] = doc[doc_id_field]
            elif 'id' in doc:
                action['_id'] = doc['id']
            elif 'ID' in doc:
                action['_id'] = doc['ID']

            actions.append(action)

        return actions

    def _execute_bulk_operation(self, actions: List[Dict[str, Any]], chunk_size: int = 1000) -> Dict[str, Any]:
        """Execute bulk operation with error handling and chunking."""
        try:
            self.logger.info(f"Executing bulk operation with {len(actions)} documents")

            success_count, failed_items = bulk(
                self.es_client,
                actions,
                chunk_size=chunk_size,
                refresh=True,
                request_timeout=300
            )

            result = {
                'success': True,
                'success_count': success_count,
                'failed_count': len(failed_items),
                'failed_items': failed_items[:10] if failed_items else []  # First 10 failures for debugging
            }

            if failed_items:
                self.logger.warning(f"{len(failed_items)} documents failed during bulk operation")
            else:
                self.logger.info(f"Successfully indexed {success_count} documents")

            return result

        except Exception as e:
            self.logger.error(f"Bulk operation failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'success_count': 0,
                'failed_count': len(actions)
            }

    def _analyze_field_structure(self, es_fields: Dict[str, Dict]) -> Dict[str, Any]:
        """Analyze field structure to identify nested vs root fields."""
        root_fields = []
        nested_fields = []

        for field_name, field_info in es_fields.items():
            if field_info.get('is_nested') or '.' in field_info.get('full_path', ''):
                nested_fields.append({
                    'name': field_name,
                    'path': field_info.get('full_path'),
                    'type': field_info.get('type')
                })
            else:
                root_fields.append({
                    'name': field_name,
                    'type': field_info.get('type')
                })

        return {
            'root_fields': root_fields,
            'nested_fields': nested_fields,
            'total_fields': len(es_fields)
        }


# Functional interface matching your original code exactly
def map_oracle_to_elastic(oracle_columns: List[str],
                          elastic_mapping: Dict[str, Any],
                          oracle_data: List[Dict[str, Any]],
                          es_client: Elasticsearch,
                          index_name: str) -> Dict[str, Any]:
    """
    Auto-detect and map Oracle columns to Elasticsearch fields, converting data format
    and pushing to ES using bulk operations. (Functional interface)

    Args:
        oracle_columns: List of Oracle column names (typically uppercase)
        elastic_mapping: Elasticsearch index mapping dictionary
        oracle_data: List of dictionaries containing Oracle row data
        es_client: Elasticsearch client instance
        index_name: Target Elasticsearch index name

    Returns:
        Dictionary with mapping results and bulk operation status
    """
    # Create mapper instance and use it
    mapper = OracleElasticsearchMapper(es_client)

    # Analyze mapping
    mapper.analyze_mapping(oracle_columns, elastic_mapping)

    # Convert data
    converted_data = mapper.convert_data(oracle_data)

    # Prepare bulk actions
    actions = mapper._prepare_bulk_actions(converted_data, index_name)

    # Execute bulk operation
    bulk_result = mapper._execute_bulk_operation(actions)

    return {
        'column_mapping': mapper.column_mapping,
        'total_records': len(oracle_data),
        'converted_records': len(converted_data),
        'bulk_result': bulk_result,
        'field_structure': mapper.field_structure
    }

