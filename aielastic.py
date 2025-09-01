#!/usr/bin/env python3
"""
FastAPI Backend for Elasticsearch Query to Questions Conversion
Reverse engineering: Elasticsearch Query → Natural Language Questions
"""

import os
import json
import logging
import time
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel, Field
from litellm import completion
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="Elasticsearch Query to Questions API",
    description="Convert Elasticsearch queries to natural language questions using LiteLLM",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "openai/Llama-4-Scout-17B-16E-Instruct")
LITELLM_MODEL_ENDPOINT = os.getenv("LITELLM_MODEL_ENDPOINT", "http://192.168.1.6:3035/v1")
LITELLM_TIMEOUT = int(os.getenv("LITELLM_TIMEOUT", "60"))
LITELLM_MAX_RETRIES = int(os.getenv("LITELLM_MAX_RETRIES", "3"))

# Request/Response Models - DEFINE THESE FIRST
class ElasticsearchQueryRequest(BaseModel):
    query: Dict[str, Any] = Field(..., description="Elasticsearch query JSON")
    index_name: Optional[str] = Field(None, description="Target index name")
    context: Optional[str] = Field(None, description="Additional context about the data")

class ElasticsearchMappingRequest(BaseModel):
    mapping: Dict[str, Any] = Field(..., description="Elasticsearch mapping JSON")
    index_name: Optional[str] = Field(None, description="Index name")
    context: Optional[str] = Field(None, description="Additional context about the data structure")

class GeneratedQuestion(BaseModel):
    question: str
    confidence: float
    explanation: str

class MappingValidationIssue(BaseModel):
    type: str  # "error", "warning", "suggestion"
    field: Optional[str]
    message: str
    recommendation: Optional[str]

class MappingValidationResponse(BaseModel):
    is_valid: bool
    overall_score: float  # 0.0 to 1.0
    issues: List[MappingValidationIssue]
    summary: str
    recommendations: List[str]
    analysis_details: Dict[str, Any]
    execution_time_ms: int
    model_used: str

class QueryToQuestionsResponse(BaseModel):
    original_query: Dict[str, Any]
    generated_questions: List[GeneratedQuestion]
    generation_method: str
    execution_time_ms: int
    model_used: str

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    litellm_available: bool
    model: str

# LiteLLM Query Analyzer Service
class QueryToQuestionsService:
    """
    Service to convert Elasticsearch queries to natural language questions
    """

    def __init__(self):
        self.base_url = LITELLM_BASE_URL
        self.model_endpoint = LITELLM_MODEL_ENDPOINT

    def analyze_query_structure(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze Elasticsearch query structure to understand intent"""
        analysis = {
            "query_type": "unknown",
            "fields_used": [],
            "conditions": [],
            "sorting": [],
            "filters": [],
            "text_search": False,
            "aggregations": False
        }

        try:
            # Analyze main query structure
            if "query" in query:
                analysis.update(self._analyze_query_clause(query["query"]))

            # Analyze sorting
            if "sort" in query:
                analysis["sorting"] = self._analyze_sort_clause(query["sort"])

            # Analyze aggregations
            if "aggs" in query or "aggregations" in query:
                analysis["aggregations"] = True

            # Analyze size and from
            if "size" in query:
                analysis["limit"] = query["size"]
            if "from" in query:
                analysis["offset"] = query["from"]

        except Exception as e:
            logger.error(f"Error analyzing query structure: {e}")

        return analysis

    def _analyze_query_clause(self, query_clause: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the main query clause"""
        analysis = {
            "query_type": "unknown",
            "fields_used": [],
            "conditions": [],
            "text_search": False
        }

        if "match" in query_clause:
            analysis["query_type"] = "text_search"
            analysis["text_search"] = True
            match_clause = query_clause["match"]
            for field, value in match_clause.items():
                analysis["fields_used"].append(field)
                if isinstance(value, dict):
                    search_value = value.get("query", value)
                else:
                    search_value = value
                analysis["conditions"].append(f"search for '{search_value}' in {field}")

        elif "multi_match" in query_clause:
            analysis["query_type"] = "multi_text_search"
            analysis["text_search"] = True
            mm_clause = query_clause["multi_match"]
            fields = mm_clause.get("fields", [])
            query_text = mm_clause.get("query", "")
            analysis["fields_used"].extend(fields)
            analysis["conditions"].append(f"search for '{query_text}' across fields: {', '.join(fields)}")

        elif "term" in query_clause:
            analysis["query_type"] = "exact_match"
            term_clause = query_clause["term"]
            for field, value in term_clause.items():
                analysis["fields_used"].append(field)
                analysis["conditions"].append(f"{field} equals '{value}'")

        elif "terms" in query_clause:
            analysis["query_type"] = "multiple_exact_match"
            terms_clause = query_clause["terms"]
            for field, values in terms_clause.items():
                analysis["fields_used"].append(field)
                analysis["conditions"].append(f"{field} is one of: {', '.join(map(str, values))}")

        elif "range" in query_clause:
            analysis["query_type"] = "range_filter"
            range_clause = query_clause["range"]
            for field, range_conditions in range_clause.items():
                analysis["fields_used"].append(field)
                condition_parts = []
                if "gte" in range_conditions:
                    condition_parts.append(f">= {range_conditions['gte']}")
                if "lte" in range_conditions:
                    condition_parts.append(f"<= {range_conditions['lte']}")
                if "gt" in range_conditions:
                    condition_parts.append(f"> {range_conditions['gt']}")
                if "lt" in range_conditions:
                    condition_parts.append(f"< {range_conditions['lt']}")
                analysis["conditions"].append(f"{field} {' and '.join(condition_parts)}")

        elif "bool" in query_clause:
            analysis["query_type"] = "compound_query"
            bool_clause = query_clause["bool"]

            # Analyze must clauses
            if "must" in bool_clause:
                for must_condition in bool_clause["must"]:
                    sub_analysis = self._analyze_query_clause(must_condition)
                    analysis["fields_used"].extend(sub_analysis["fields_used"])
                    analysis["conditions"].extend([f"MUST: {cond}" for cond in sub_analysis["conditions"]])
                    if sub_analysis["text_search"]:
                        analysis["text_search"] = True

            # Analyze should clauses
            if "should" in bool_clause:
                for should_condition in bool_clause["should"]:
                    sub_analysis = self._analyze_query_clause(should_condition)
                    analysis["fields_used"].extend(sub_analysis["fields_used"])
                    analysis["conditions"].extend([f"SHOULD: {cond}" for cond in sub_analysis["conditions"]])

            # Analyze filter clauses
            if "filter" in bool_clause:
                for filter_condition in bool_clause["filter"]:
                    sub_analysis = self._analyze_query_clause(filter_condition)
                    analysis["fields_used"].extend(sub_analysis["fields_used"])
                    analysis["conditions"].extend([f"FILTER: {cond}" for cond in sub_analysis["conditions"]])

            # Analyze must_not clauses
            if "must_not" in bool_clause:
                for must_not_condition in bool_clause["must_not"]:
                    sub_analysis = self._analyze_query_clause(must_not_condition)
                    analysis["fields_used"].extend(sub_analysis["fields_used"])
                    analysis["conditions"].extend([f"MUST NOT: {cond}" for cond in sub_analysis["conditions"]])

        elif "match_all" in query_clause:
            analysis["query_type"] = "match_all"
            analysis["conditions"].append("match all documents")

        return analysis

    def _analyze_sort_clause(self, sort_clause: List[Dict[str, Any]]) -> List[str]:
        """Analyze sorting clause"""
        sorting_info = []

        for sort_item in sort_clause:
            if isinstance(sort_item, dict):
                for field, sort_config in sort_item.items():
                    if isinstance(sort_config, dict):
                        order = sort_config.get("order", "asc")
                        sorting_info.append(f"sort by {field} ({order}ending)")
                    else:
                        sorting_info.append(f"sort by {field}")
            else:
                sorting_info.append(f"sort by {sort_item}")

        return sorting_info

    def call_litellm_with_retry(self, messages: List[Dict], max_retries: int = LITELLM_MAX_RETRIES) -> Optional[str]:
        """Call LiteLLM with retry logic using local proxy server"""

        for attempt in range(max_retries):
            try:
                logger.info(f"LiteLLM attempt {attempt + 1}/{max_retries} using endpoint: {self.model_endpoint}")

                response = completion(
                    base_url=self.base_url,
                    model=self.model_endpoint,
                    messages=messages,
                    temperature=0.3,  # Some creativity for question generation
                    max_tokens=1000,
                    timeout=LITELLM_TIMEOUT
                )

                if response and "choices" in response and len(response["choices"]) > 0:
                    content = response["choices"][0]["message"]["content"].strip()
                    if content:
                        logger.info(f"LiteLLM success: {len(content)} characters")
                        return content
                    else:
                        logger.warning("Empty response from LiteLLM")

                else:
                    logger.warning("No valid response from LiteLLM")

            except Exception as e:
                logger.error(f"LiteLLM error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

        logger.error("All LiteLLM attempts failed")
        return None

    def generate_questions_from_query(self, query: Dict[str, Any], context: Optional[str] = None) -> List[GeneratedQuestion]:
        """Generate natural language questions from Elasticsearch query"""

        # Analyze query structure
        analysis = self.analyze_query_structure(query)

        # Build system prompt
        system_prompt = self._build_system_prompt()

        # Build user prompt
        user_prompt = self._build_user_prompt(query, analysis, context)

        # Call LiteLLM
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        response = self.call_litellm_with_retry(messages)

        if response:
            return self._parse_questions_response(response)
        else:
            return self._generate_fallback_questions(analysis)

    def _build_system_prompt(self) -> str:
        """Build system prompt for question generation"""
        return """You are an expert at understanding Elasticsearch queries and generating natural language questions that would lead to those queries.

Your task is to analyze an Elasticsearch query and generate realistic, natural questions that a user might ask to produce that query.

Guidelines:
1. Generate 3-5 different questions with varying complexity
2. Make questions sound natural and conversational
3. Consider different ways users might phrase the same intent
4. Include confidence scores (0.0-1.0) based on how likely each question is
5. Provide brief explanations for each question

Response format:
```
Question 1: [Natural language question]
Confidence: [0.0-1.0]
Explanation: [Why this question would generate this query]

Question 2: [Natural language question]
Confidence: [0.0-1.0]
Explanation: [Why this question would generate this query]
```

Focus on making questions that real users would actually ask."""

    def _build_user_prompt(self, query: Dict[str, Any], analysis: Dict[str, Any], context: Optional[str] = None) -> str:
        """Build user prompt with query and analysis"""

        # Create human-readable query summary
        query_summary = self._create_query_summary(analysis)

        context_info = f"\nContext about the data: {context}" if context else ""

        return f"""Analyze this Elasticsearch query and generate natural language questions:

ELASTICSEARCH QUERY:
```json
{json.dumps(query, indent=2)}
```

QUERY ANALYSIS:
- Type: {analysis.get('query_type', 'unknown')}
- Fields used: {', '.join(analysis.get('fields_used', []))}
- Conditions: {'; '.join(analysis.get('conditions', []))}
- Sorting: {'; '.join(analysis.get('sorting', []))}
- Text search: {analysis.get('text_search', False)}
{context_info}

Generate 3-5 natural language questions that would lead to this Elasticsearch query. Consider different ways users might ask for the same information.

Examples of good question generation:
- Query with price range → "Find items under $100" or "Show me affordable options"
- Query with text search → "Search for wireless headphones" or "Find products about wireless audio"
- Query with ratings → "Show me highly rated items" or "Find products with good reviews"

Provide questions in the specified format with confidence scores and explanations."""

    def _create_query_summary(self, analysis: Dict[str, Any]) -> str:
        """Create human-readable summary of query"""
        summary_parts = []

        if analysis.get("text_search"):
            summary_parts.append("searches text content")

        if analysis.get("conditions"):
            summary_parts.append(f"filters by: {', '.join(analysis['conditions'])}")

        if analysis.get("sorting"):
            summary_parts.append(f"sorted by: {', '.join(analysis['sorting'])}")

        return "; ".join(summary_parts) if summary_parts else "matches all documents"

    def _parse_questions_response(self, response: str) -> List[GeneratedQuestion]:
        """Parse LiteLLM response into structured questions"""
        questions = []

        try:
            # Split response into question blocks
            blocks = response.split("Question ")

            for block in blocks[1:]:  # Skip first empty block
                try:
                    lines = block.strip().split('\n')
                    question_line = lines[0] if lines else ""

                    # Extract question (remove numbering)
                    question = question_line.split(':', 1)[1].strip() if ':' in question_line else question_line.strip()

                    # Extract confidence
                    confidence = 0.5  # default
                    confidence_line = next((line for line in lines if line.strip().startswith('Confidence:')), '')
                    if confidence_line:
                        try:
                            confidence_str = confidence_line.split(':', 1)[1].strip()
                            confidence = float(confidence_str)
                        except (ValueError, IndexError):
                            pass

                    # Extract explanation
                    explanation = "Generated based on query analysis"
                    explanation_line = next((line for line in lines if line.strip().startswith('Explanation:')), '')
                    if explanation_line:
                        explanation = explanation_line.split(':', 1)[1].strip()

                    if question:
                        questions.append(GeneratedQuestion(
                            question=question,
                            confidence=confidence,
                            explanation=explanation
                        ))

                except Exception as e:
                    logger.warning(f"Error parsing question block: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error parsing questions response: {e}")

        return questions if questions else self._generate_fallback_questions({})

    def _generate_fallback_questions(self, analysis: Dict[str, Any]) -> List[GeneratedQuestion]:
        """Generate fallback questions when LiteLLM fails"""
        fallback_questions = [
            GeneratedQuestion(
                question="Find all matching items",
                confidence=0.3,
                explanation="Generic fallback question"
            ),
            GeneratedQuestion(
                question="Search for relevant results",
                confidence=0.3,
                explanation="Basic search fallback"
            )
        ]

        # Add some analysis-based questions
        if analysis.get("text_search"):
            fallback_questions.append(GeneratedQuestion(
                question="Search for specific content",
                confidence=0.4,
                explanation="Based on text search detection"
            ))

        if "price" in analysis.get("fields_used", []):
            fallback_questions.append(GeneratedQuestion(
                question="Find items by price",
                confidence=0.4,
                explanation="Based on price field usage"
            ))

        return fallback_questions

# LiteLLM Mapping Validation Service - NOW DEFINED AFTER MODELS
class MappingValidationService:
    """
    Service to validate Elasticsearch mappings using LLM analysis
    """

    def __init__(self):
        self.base_url = LITELLM_BASE_URL
        self.model_endpoint = LITELLM_MODEL_ENDPOINT

        # Known Elasticsearch field types
        self.valid_field_types = {
            'text', 'keyword', 'integer', 'long', 'short', 'byte', 'double', 'float', 'half_float', 'scaled_float',
            'date', 'boolean', 'binary', 'integer_range', 'float_range', 'long_range', 'double_range', 'date_range',
            'ip', 'completion', 'token_count', 'murmur3', 'annotated-text', 'percolator', 'join', 'rank_feature',
            'rank_features', 'dense_vector', 'sparse_vector', 'search_as_you_type', 'alias', 'flattened',
            'nested', 'object', 'geo_point', 'geo_shape', 'point', 'shape', 'histogram'
        }

        # Best practices patterns
        self.best_practices = {
            'analyzers': ['standard', 'simple', 'whitespace', 'stop', 'keyword', 'pattern', 'language'],
            'text_fields_should_have_keyword': True,
            'avoid_deep_nesting': 3,  # Max nesting levels
            'max_fields_per_index': 1000,
            'date_format_patterns': ['yyyy-MM-dd', 'yyyy-MM-dd HH:mm:ss', 'epoch_millis', 'epoch_second']
        }

    def call_litellm_with_retry(self, messages: List[Dict], max_retries: int = LITELLM_MAX_RETRIES) -> Optional[str]:
        """Call LiteLLM with retry logic using local proxy server"""

        for attempt in range(max_retries):
            try:
                logger.info(f"LiteLLM mapping validation attempt {attempt + 1}/{max_retries}")

                response = completion(
                    base_url=self.base_url,
                    model=self.model_endpoint,
                    messages=messages,
                    temperature=0.1,  # Low temperature for consistent validation
                    max_tokens=1500,
                    timeout=LITELLM_TIMEOUT
                )

                if response and "choices" in response and len(response["choices"]) > 0:
                    content = response["choices"][0]["message"]["content"].strip()
                    if content:
                        logger.info(f"LiteLLM mapping validation success: {len(content)} characters")
                        return content
                    else:
                        logger.warning("Empty response from LiteLLM")

                else:
                    logger.warning("No valid response from LiteLLM")

            except Exception as e:
                logger.error(f"LiteLLM mapping validation error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

        logger.error("All LiteLLM mapping validation attempts failed")
        return None

    def validate_mapping_structure(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Perform structural validation of the mapping"""
        validation_result = {
            "structural_issues": [],
            "field_type_issues": [],
            "best_practice_issues": [],
            "field_count": 0,
            "nesting_depth": 0,
            "has_text_fields": False,
            "has_keyword_fields": False
        }

        try:
            # Check basic structure
            if "mappings" not in mapping and "properties" not in mapping:
                validation_result["structural_issues"].append({
                    "type": "error",
                    "field": None,
                    "message": "Missing 'mappings' or 'properties' section",
                    "recommendation": "Add 'mappings' section with 'properties'"
                })
                return validation_result

            # Extract properties
            properties = mapping.get("mappings", {}).get("properties", mapping.get("properties", {}))

            if not properties:
                validation_result["structural_issues"].append({
                    "type": "error",
                    "field": None,
                    "message": "No field properties defined",
                    "recommendation": "Define at least one field in properties"
                })
                return validation_result

            # Analyze fields recursively
            validation_result = self._analyze_fields_recursive(properties, validation_result, "", 0)

            # Check best practices
            self._check_best_practices(validation_result, properties)

        except Exception as e:
            logger.error(f"Error in structural validation: {e}")
            validation_result["structural_issues"].append({
                "type": "error",
                "field": None,
                "message": f"Validation error: {str(e)}",
                "recommendation": "Check mapping JSON structure"
            })

        return validation_result

    def _analyze_fields_recursive(self, properties: Dict[str, Any], validation_result: Dict[str, Any],
                                  parent_path: str, depth: int) -> Dict[str, Any]:
        """Recursively analyze field properties - FIXED VERSION"""

        validation_result["nesting_depth"] = max(validation_result["nesting_depth"], depth)

        for field_name, field_config in properties.items():
            current_path = f"{parent_path}.{field_name}" if parent_path else field_name
            validation_result["field_count"] += 1

            if not isinstance(field_config, dict):
                validation_result["field_type_issues"].append({
                    "type": "error",
                    "field": current_path,
                    "message": "Field configuration must be an object",
                    "recommendation": "Define field properties as JSON object"
                })
                continue

            field_type = field_config.get("type")

            # CRITICAL FIX: Check for missing field type
            if field_type is None:
                # Check if this is an object field with properties (valid case)
                if "properties" in field_config:
                    # This is a valid object field, continue processing
                    field_type = "object"
                else:
                    # This is an invalid field with no type and no properties
                    validation_result["field_type_issues"].append({
                        "type": "error",
                        "field": current_path,
                        "message": "Field is missing required 'type' property",
                        "recommendation": f"Add field type, e.g., 'type': 'text', 'integer', 'keyword', or 'date'"
                    })
                    continue

            # Check field type validity (if type is specified)
            if field_type and field_type not in self.valid_field_types:
                validation_result["field_type_issues"].append({
                    "type": "error",
                    "field": current_path,
                    "message": f"Invalid field type: '{field_type}'",
                    "recommendation": f"Use valid Elasticsearch field types: {', '.join(sorted(list(self.valid_field_types)[:10]))}..."
                })
                continue

            # Track field types
            if field_type == "text":
                validation_result["has_text_fields"] = True

                # Check if text field has keyword subfield
                if "fields" not in field_config or "keyword" not in field_config.get("fields", {}):
                    validation_result["best_practice_issues"].append({
                        "type": "suggestion",
                        "field": current_path,
                        "message": "Text field should have keyword subfield for sorting/aggregations",
                        "recommendation": "Add keyword subfield: 'fields': {'keyword': {'type': 'keyword'}}"
                    })

            elif field_type == "keyword":
                validation_result["has_keyword_fields"] = True

                # Check for invalid analyzer usage on keyword fields
                if "analyzer" in field_config:
                    validation_result["best_practice_issues"].append({
                        "type": "warning",
                        "field": current_path,
                        "message": "Keyword fields should not use analyzers",
                        "recommendation": "Remove 'analyzer' property from keyword field or change type to 'text'"
                    })

            # Check for nested objects
            if field_type == "object" or "properties" in field_config:
                nested_properties = field_config.get("properties", {})
                if nested_properties:
                    validation_result = self._analyze_fields_recursive(
                        nested_properties, validation_result, current_path, depth + 1
                    )

            # Check date field formats
            if field_type == "date":
                format_value = field_config.get("format")
                if not format_value:
                    validation_result["best_practice_issues"].append({
                        "type": "suggestion",
                        "field": current_path,
                        "message": "Date field should specify format",
                        "recommendation": "Add format specification, e.g., 'format': 'yyyy-MM-dd'"
                    })

            # Check for uppercase field names (best practice)
            if field_name.isupper():
                validation_result["best_practice_issues"].append({
                    "type": "suggestion",
                    "field": current_path,
                    "message": f"Field name '{field_name}' uses uppercase - lowercase is preferred",
                    "recommendation": f"Consider using '{field_name.lower()}' instead"
                })

            # Check for abbreviated field names
            if field_name in ["DOB", "SSN", "ID", "NUM", "DESC"]:
                expanded_names = {
                    "DOB": "date_of_birth",
                    "SSN": "social_security_number",
                    "ID": "identifier",
                    "NUM": "number",
                    "DESC": "description"
                }
                validation_result["best_practice_issues"].append({
                    "type": "suggestion",
                    "field": current_path,
                    "message": f"Abbreviated field name '{field_name}' should be more descriptive",
                    "recommendation": f"Consider using '{expanded_names.get(field_name, field_name.lower())}' for clarity"
                })

        return validation_result

    def _check_best_practices(self, validation_result: Dict[str, Any], properties: Dict[str, Any]):
        """Check for best practice violations"""

        # Check field count
        if validation_result["field_count"] > self.best_practices["max_fields_per_index"]:
            validation_result["best_practice_issues"].append({
                "type": "warning",
                "field": None,
                "message": f"High field count: {validation_result['field_count']} fields",
                "recommendation": f"Consider reducing fields below {self.best_practices['max_fields_per_index']} for better performance"
            })

        # Check nesting depth
        if validation_result["nesting_depth"] > self.best_practices["avoid_deep_nesting"]:
            validation_result["best_practice_issues"].append({
                "type": "warning",
                "field": None,
                "message": f"Deep nesting detected: {validation_result['nesting_depth']} levels",
                "recommendation": f"Consider flattening structure or using nested type for objects deeper than {self.best_practices['avoid_deep_nesting']} levels"
            })

    def validate_mapping_with_llm(self, mapping: Dict[str, Any], context: Optional[str] = None) -> Dict[str, Any]:
        """Use LLM to validate mapping and provide intelligent feedback"""

        # Build system prompt
        system_prompt = """You are an expert Elasticsearch mapping analyst. Analyze the mapping and provide validation feedback.

Respond in this format:
VALIDATION_SUMMARY: [VALID/INVALID] - [Brief assessment]
CRITICAL_ISSUES:
- [Critical problems]
WARNINGS:
- [Performance concerns]
SUGGESTIONS:
- [Recommendations]
SCORE: [0-100] - [Quality score]
RECOMMENDATIONS:
1. [Specific recommendations]"""

        # Build user prompt
        user_prompt = f"""Analyze this Elasticsearch mapping:

```json
{json.dumps(mapping, indent=2)}
```

Context: {context or "General mapping"}

Provide validation analysis focusing on field types, performance, and best practices."""

        # Call LiteLLM
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        response = self.call_litellm_with_retry(messages)

        if response:
            return self._parse_mapping_validation_response(response)
        else:
            return self._generate_fallback_validation_result()

    def _parse_mapping_validation_response(self, response: str) -> Dict[str, Any]:
        """Parse LLM response into structured validation result"""

        try:
            result = {
                "is_valid": True,
                "overall_score": 0.5,
                "summary": "",
                "critical_issues": [],
                "warnings": [],
                "suggestions": [],
                "detailed_analysis": {},
                "recommendations": []
            }

            lines = response.split('\n')
            current_section = None

            for line in lines:
                line = line.strip()

                if line.startswith('VALIDATION_SUMMARY:'):
                    summary_text = line.replace('VALIDATION_SUMMARY:', '').strip()
                    result["summary"] = summary_text
                    result["is_valid"] = "VALID" in summary_text.upper()

                elif line.startswith('CRITICAL_ISSUES:'):
                    current_section = "critical_issues"

                elif line.startswith('WARNINGS:'):
                    current_section = "warnings"

                elif line.startswith('SUGGESTIONS:'):
                    current_section = "suggestions"

                elif line.startswith('SCORE:'):
                    score_text = line.replace('SCORE:', '').strip()
                    try:
                        score_match = re.search(r'(\d+)', score_text)
                        if score_match:
                            result["overall_score"] = int(score_match.group(1)) / 100.0
                    except:
                        pass

                elif line.startswith('RECOMMENDATIONS:'):
                    current_section = "recommendations"

                elif line.startswith('- ') and current_section:
                    item = line[2:].strip()
                    if current_section in ["critical_issues", "warnings", "suggestions", "recommendations"]:
                        result[current_section].append(item)

            return result

        except Exception as e:
            logger.error(f"Error parsing mapping validation response: {e}")
            return self._generate_fallback_validation_result()

    def _generate_fallback_validation_result(self) -> Dict[str, Any]:
        """Generate fallback validation result when LLM fails"""
        return {
            "is_valid": True,
            "overall_score": 0.5,
            "summary": "Basic validation completed - LLM analysis unavailable",
            "critical_issues": [],
            "warnings": ["Could not perform advanced LLM validation"],
            "suggestions": ["Ensure mapping follows Elasticsearch best practices"],
            "detailed_analysis": {"analysis_method": "fallback"},
            "recommendations": ["Review field types and mapping structure manually"]
        }


    def validate_mapping_comprehensive(self, mapping: Dict[str, Any], context: Optional[str] = None) -> MappingValidationResponse:
        """Perform comprehensive mapping validation combining structural and LLM analysis - FIXED VERSION"""

        start_time = datetime.now()

        try:
            # Perform structural validation
            structural_validation = self.validate_mapping_structure(mapping)

            # Count issues by type
            error_count = (len(structural_validation.get("structural_issues", [])) +
                           len([i for i in structural_validation.get("field_type_issues", []) if i["type"] == "error"]))

            # If there are critical structural errors, don't proceed with LLM validation
            if error_count > 0:
                logger.warning(f"Skipping LLM validation due to {error_count} structural errors")
                llm_validation = {
                    "is_valid": False,
                    "overall_score": 0.2,
                    "summary": f"Invalid mapping - {error_count} critical errors found",
                    "critical_issues": [f"Found {error_count} field configuration errors"],
                    "warnings": [],
                    "suggestions": ["Fix field type definitions before proceeding"],
                    "detailed_analysis": {"analysis_method": "structural_only"},
                    "recommendations": ["Define proper field types for all properties"]
                }
            else:
                # Perform LLM validation only if structural validation passes
                llm_validation = self.validate_mapping_with_llm(mapping, context)

            # Combine results
            all_issues = []

            # Add structural issues
            for issue_list in [structural_validation.get("structural_issues", []),
                               structural_validation.get("field_type_issues", []),
                               structural_validation.get("best_practice_issues", [])]:
                for issue in issue_list:
                    all_issues.append(MappingValidationIssue(
                        type=issue["type"],
                        field=issue["field"],
                        message=issue["message"],
                        recommendation=issue.get("recommendation")
                    ))

            # Add LLM issues (only if no critical structural errors)
            if error_count == 0:
                for issue_type in ["critical_issues", "warnings", "suggestions"]:
                    for issue_text in llm_validation.get(issue_type, []):
                        issue_severity = "error" if issue_type == "critical_issues" else \
                            "warning" if issue_type == "warnings" else "suggestion"

                        all_issues.append(MappingValidationIssue(
                            type=issue_severity,
                            field=None,
                            message=issue_text,
                            recommendation=None
                        ))

            # Determine overall validity - FIXED LOGIC
            has_errors = any(issue.type == "error" for issue in all_issues)
            is_valid = not has_errors  # Simple: if there are errors, it's invalid

            # Calculate overall score - FIXED LOGIC
            if has_errors:
                overall_score = 0.1  # Very low score for invalid mappings
            else:
                structural_score = 1.0 - (len([i for i in all_issues if i.type == "warning"]) * 0.1)
                llm_score = llm_validation.get("overall_score", 0.7)
                overall_score = max(0.1, min(1.0, (structural_score + llm_score) / 2))

            # Build analysis details
            analysis_details = {
                "structural_validation": {
                    "field_count": structural_validation.get("field_count", 0),
                    "nesting_depth": structural_validation.get("nesting_depth", 0),
                    "has_text_fields": structural_validation.get("has_text_fields", False),
                    "has_keyword_fields": structural_validation.get("has_keyword_fields", False),
                    "error_count": error_count
                },
                "llm_analysis": llm_validation.get("detailed_analysis", {}),
                "validation_method": "comprehensive"
            }

            # Execution time
            execution_time = (datetime.now() - start_time).total_seconds() * 1000

            return MappingValidationResponse(
                is_valid=is_valid,
                overall_score=overall_score,
                issues=all_issues,
                summary=llm_validation.get("summary", "Mapping validation completed"),
                recommendations=llm_validation.get("recommendations", []),
                analysis_details=analysis_details,
                execution_time_ms=int(execution_time),
                model_used=f"{LITELLM_BASE_URL} -> {LITELLM_MODEL_ENDPOINT}"
            )

        except Exception as e:
            logger.error(f"Error in comprehensive mapping validation: {e}")

            # Return error response
            execution_time = (datetime.now() - start_time).total_seconds() * 1000

            return MappingValidationResponse(
                is_valid=False,
                overall_score=0.0,
                issues=[MappingValidationIssue(
                    type="error",
                    field=None,
                    message=f"Validation failed: {str(e)}",
                    recommendation="Check mapping structure and try again"
                )],
                summary=f"Validation error: {str(e)}",
                recommendations=["Review mapping syntax and structure"],
                analysis_details={"error": str(e)},
                execution_time_ms=int(execution_time),
                model_used=f"{LITELLM_BASE_URL} -> {LITELLM_MODEL_ENDPOINT}"
            )





# Initialize services AFTER all classes are defined
query_service = QueryToQuestionsService()
mapping_service = MappingValidationService()



async def validate_elasticsearch_mapping(request: ElasticsearchMappingRequest):
    """
    Validate Elasticsearch mapping using LLM analysis
    """
    try:
        logger.info(f"Processing mapping validation request")

        # Validate mapping structure
        if not request.mapping or not isinstance(request.mapping, dict):
            raise HTTPException(status_code=400, detail="Invalid mapping format - must be a JSON object")

        # Perform comprehensive validation
        validation_result = mapping_service.validate_mapping_comprehensive(
            mapping=request.mapping,
            context=request.context
        )

        logger.info(f"Mapping validation completed: {'valid' if validation_result.is_valid else 'invalid'}, "
                    f"score: {validation_result.overall_score:.2f}, "
                    f"issues: {len(validation_result.issues)}")

        return validation_result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in mapping validation: {e}")
        raise HTTPException(status_code=500, detail=f"Mapping validation failed: {str(e)}")


async def convert_query_to_questions(request: ElasticsearchQueryRequest):
    """
    Convert Elasticsearch query to natural language questions
    """
    start_time = datetime.now()

    try:
        logger.info(f"Processing query to questions conversion")

        # Validate query structure
        if not request.query or not isinstance(request.query, dict):
            raise HTTPException(status_code=400, detail="Invalid Elasticsearch query format")

        # Generate questions
        questions = query_service.generate_questions_from_query(
            query=request.query,
            context=request.context
        )

        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds() * 1000

        logger.info(f"Generated {len(questions)} questions in {execution_time:.0f}ms")

        return QueryToQuestionsResponse(
            original_query=request.query,
            generated_questions=questions,
            generation_method="litellm" if questions and questions[0].confidence > 0.4 else "fallback",
            execution_time_ms=int(execution_time),
            model_used=f"{LITELLM_BASE_URL} -> {LITELLM_MODEL_ENDPOINT}"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in query to questions conversion: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def get_sample_mappings():
    """Get sample Elasticsearch mappings for testing"""

    sample_mappings = [
        {
            "name": "E-commerce Products",
            "description": "Mapping for online store products",
            "mapping": {
                "mappings": {
                    "properties": {
                        "title": {
                            "type": "text",
                            "analyzer": "standard",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "description": {
                            "type": "text",
                            "analyzer": "standard"
                        },
                        "price": {
                            "type": "float"
                        },
                        "category": {
                            "type": "keyword"
                        },
                        "brand": {
                            "type": "keyword"
                        },
                        "rating": {
                            "type": "float"
                        },
                        "reviews_count": {
                            "type": "integer"
                        },
                        "in_stock": {
                            "type": "boolean"
                        },
                        "created_date": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss"
                        },
                        "tags": {
                            "type": "keyword"
                        }
                    }
                }
            }
        },
        {
            "name": "Invalid Mapping Example",
            "description": "Example with validation issues",
            "mapping": {
                "mappings": {
                    "properties": {
                        "title": {
                            "type": "text"
                            # Missing keyword subfield
                        },
                        "price": {
                            "type": "invalid_type"  # Invalid field type
                        },
                        "category": {
                            "type": "keyword",
                            "analyzer": "standard"  # Analyzer on keyword field
                        }
                    }
                }
            }
        }
    ]

    return {"sample_mappings": sample_mappings}


async def get_sample_queries():
    """Get sample Elasticsearch queries for testing"""

    sample_queries = [
        {
            "name": "Text Search",
            "description": "Search for wireless headphones",
            "query": {
                "query": {
                    "match": {
                        "title": "wireless headphones"
                    }
                },
                "sort": [{"_score": {"order": "desc"}}],
                "size": 20
            }
        },
        {
            "name": "Price Range Filter",
            "description": "Find items under $100",
            "query": {
                "query": {
                    "range": {
                        "price": {
                            "lte": 100
                        }
                    }
                },
                "sort": [{"price": {"order": "asc"}}],
                "size": 20
            }
        }
    ]

    return {"sample_queries": sample_queries}
