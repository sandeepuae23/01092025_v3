import json

def condition_to_es(condition):
    field = condition["field"]
    operator = condition["operator"]
    value = condition.get("value")
    boost = condition.get("boost")
    field_type = condition.get("field_type", "keyword")
    if operator == "==":
        query = {"term": {field: {"value": value}}}
        if boost:
            query["term"][field]["boost"] = boost
    elif operator == "!=":
        return {"bool": {"must_not": [{"term": {field: {"value": value}}}]}}
    elif operator == ">":
        query = {"range": {field: {"gt": value}}}
    elif operator == ">=":
        query = {"range": {field: {"gte": value}}}
    elif operator == "<":
        query = {"range": {field: {"lt": value}}}
    elif operator == "<=":
        query = {"range": {field: {"lte": value}}}

    elif operator == "range":
        if not isinstance(value, dict):
            raise ValueError("Range operator requires dict with 'gte' and/or 'lte' keys")

        range_query = {}

        # Add gte if provided
        if "gte" in value:
            range_query["gte"] = value["gte"]

        # Add lte if provided
        if "lte" in value:
            range_query["lte"] = value["lte"]

        # Add gt if provided
        if "gt" in value:
            range_query["gt"] = value["gt"]

        # Add lt if provided
        if "lt" in value:
            range_query["lt"] = value["lt"]
        query = {"range": {field: range_query}}

    elif operator == "wildcard":
        query = {"wildcard": {field: {"value": value}}}
        if boost:
            query["wildcard"][field]["boost"] = boost
    elif operator == "match":
        query = {"match": {field: {"query": value}}}
        if boost:
            query["match"][field]["boost"] = boost
    elif operator == "in":
        if not isinstance(value, list):
            raise ValueError("The 'in' operator expects a list of values.")
        query = {"terms": {field: value}}
    elif operator == "between":
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise ValueError("Between needs two values.")
        range_query = {"gte": value[0], "lte": value[1]}
        if field_type in ("date", "datetime"):
            range_query["format"] = "strict_date_optional_time"
        query = {"range": {field: range_query}}
    elif operator == "exists":
        query = {"exists": {"field": field}}
    elif operator == "missing":
        query = {"bool": {"must_not": [{"exists": {"field": field}}]}}
    else:
        raise ValueError(f"Unsupported operator: {operator}")

    return query


def group_to_es(group, inherited_nested_path=None):
    logic = group["operator"]
    bool_key = "must" if logic == "AND" else "should"

    clauses = []
    current_nested_path = group.get("nested_path", inherited_nested_path)

    for condition in group.get("conditions", []):
        clauses.append(condition_to_es(condition))

    for subgroup in group.get("groups", []):
        # If subgroup has its own nested_path, treat it as a new nested group
        if "nested_path" in subgroup:
            clauses.append(group_to_es(subgroup, inherited_nested_path=current_nested_path))
        else:
            # Inherit the current nested path

            clauses.append(group_to_es(subgroup, inherited_nested_path=None))

    group_query = {"bool": {bool_key: clauses}}

    if bool_key == "should":
        group_query["bool"]["minimum_should_match"] = 1

    if current_nested_path:
        group_query = {
            "nested": {
                "path": current_nested_path,
                "query": group_query
            }
        }

    if group.get("negate"):
        group_query = {"bool": {"must_not": [group_query]}}

    return group_query


def build_es_query_v3(query_structure, query_aggregations=None):
    query_body = query_structure["query"]
    query = {
        "track_total_hits": True,
        "query": group_to_es_V2(query_body)
    }

    pagination = query_structure.get("pagination")
    if pagination:
        query["from"] = pagination.get("from", 0)
        query["size"] = pagination.get("size", 10)



    aggs = query_aggregations.get("aggregations") if query_aggregations else None
    if aggs:
        query["aggs"] = build_es_aggregations(aggs)

    source_fields = query_structure.get("_source")
    if source_fields:
        query["_source"] = source_fields

    return query


def group_to_es_V2(group, inherited_nested_path=None):
    logic = group["operator"]
    bool_key = (
        "must" if logic == "AND" else
        "should" if logic == "OR" else
        "must_not" if logic == "NOT" else
        "must"
    )

    clauses = []
    current_nested_path = group.get("nested_path", inherited_nested_path)
    is_has_child = group.get("has_child_type")

    for condition in group.get("conditions", []):
        clauses.append(condition_to_es(condition))

    for subgroup in group.get("groups", []):
        if "nested_path" in subgroup:
            # Use new nested path from subgroup
            clauses.append(group_to_es_V2(subgroup, inherited_nested_path=subgroup["nested_path"]))
        elif "has_child_type" in subgroup:
            # Subgroup defines a has_child block
            clauses.append(group_to_es_V2(subgroup, inherited_nested_path=subgroup["has_child_type"]))
        else:
            # Inherit current path (if any)
            clauses.append(group_to_es_V2(subgroup, inherited_nested_path=current_nested_path))

    group_query = {"bool": {bool_key: clauses}}
    if bool_key == "should":
        group_query["bool"]["minimum_should_match"] = 1

    # Handle has_child wrapping
    if is_has_child:
        group_query = {
            "has_child": {
                "type": is_has_child,
                "query": group_query
            }
        }

    # Handle nested wrapping
    elif current_nested_path:
        group_query = {
            "nested": {
                "path": current_nested_path,
                "query": group_query
            }
        }

    # Handle negation
    if group.get("negate"):
        group_query = {"bool": {"must_not": [group_query]}}

    return group_query




def build_es_query_v2(query_structure):
    if "query" not in query_structure:
        raise ValueError("Missing 'query' key in top-level query structure")

    query_body = query_structure["query"]

    query = {
        "track_total_hits": True,
        "query": group_to_es(query_body)
    }

    pagination = query_body.get("pagination")
    if pagination:
        query["from"] = pagination.get("from", 0)
        query["size"] = pagination.get("size", 10)

    sort_rules = []
    for sort_field in query_body.get("sort", []):
        sort_rules.append({sort_field["field"]: {"order": sort_field["order"]}})
    if sort_rules:
        query["sort"] = sort_rules

    aggs = query_body.get("aggregations")
    if aggs:
        query["aggs"] = build_aggregations(aggs)

    source_fields = query_body.get("_source")
    if source_fields:
        query["_source"] = source_fields

    print(json.dumps(query, indent=2))
    return query




def build_aggregations(aggs_config):
    result = {}

    for name, config in aggs_config.items():
        agg_body = {}
        nested_aggs = None

        for key, value in config.items():
            if key == "aggs":
                nested_aggs = value
            else:
                agg_body[key] = value

        result[name] = agg_body

        if nested_aggs:
            result[name]["aggs"] = build_aggregations(nested_aggs)

    return result

def build_es_aggregations(aggs_list):
    def build_nested_aggs(agg):
        if "nested_path" in agg and agg["nested_path"]:
            # Handle nested path and recursively build inner aggregation
            return {
                "nested": {
                    "path": agg["nested_path"]
                },
                "aggs": {
                    agg["name"]: build_nested_aggs({
                        key: value for key, value in agg.items() if key != "nested_path"
                    })
                }
            }

        # Handle leaf aggregations
        if agg["type"] == "terms":
            result = {
                "terms": {
                    "field": agg["field"]
                }
            }
        elif agg["type"] == "avg":
            result = {
                "avg": {
                    "field": agg["field"]
                }
            }
        elif agg["type"] == "sum":
            result = {
                "sum": {
                    "field": agg["field"]
                }
            }
        else:
            raise ValueError(f"Unsupported aggregation type: {agg['type']}")

        # Handle sub-aggregations (only if not a nested type already)
        if "sub_aggregations" in agg:
            result["aggs"] = {}
            for sub in agg["sub_aggregations"]:
                result["aggs"][sub["name"]] = build_nested_aggs(sub)

        return result

    es_aggs = {}
    for agg in aggs_list:
        es_aggs[agg["name"]] = build_nested_aggs(agg)

    return es_aggs


