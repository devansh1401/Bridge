def mongo_find_to_sql(mongo_obj: dict) -> str:
    """
    Convert a Mongo-style query dict into a SQL SELECT statement.

    Supports:
      - Simple find dict → WHERE
      - projection → SELECT columns
      - sort → ORDER BY
      - limit, skip → LIMIT/OFFSET
      - group key → GROUP BY + COUNT
      - pipeline key ($lookup) → JOIN conversion

    :param mongo_obj: The MongoDB query dict or aggregation config.
    :return: A SQL SELECT query string.
    """
    # Handle GROUP BY conversion
    if 'group' in mongo_obj:
        grp = mongo_obj['group'].get('$group', {})
        _id = grp.get('_id', {})
        if not isinstance(_id, dict) or not _id:
            raise ValueError("Invalid 'group' structure; expected '_id' dict.")
        cols = list(_id.keys())
        cols_str = ", ".join(cols)
        sql = f"SELECT {cols_str}, COUNT(*) FROM {mongo_obj.get('collection')}"
        where_sql = build_where_sql(mongo_obj.get('find', {}))
        if where_sql:
            sql += f" WHERE {where_sql}"
        order_sql = build_order_by_sql(mongo_obj.get('sort', []))
        if order_sql:
            sql += f" GROUP BY {cols_str} ORDER BY {order_sql}"
        else:
            sql += f" GROUP BY {cols_str}"
        lim = mongo_obj.get('limit')
        if isinstance(lim, int) and lim > 0:
            sql += f" LIMIT {lim}"
        return sql + ";"

    # If this is an aggregation pipeline (JOIN), delegate to the join handler
    if 'pipeline' in mongo_obj:
        return _handle_join_pipeline(mongo_obj)

    # Validate input structure
    if not isinstance(mongo_obj, dict):
        raise TypeError("Input must be a dict representing a MongoDB query.")
    if 'collection' not in mongo_obj or not isinstance(mongo_obj['collection'], str):
        raise ValueError("Invalid MongoDB query: missing or invalid 'collection' key.")
    if 'find' not in mongo_obj or not isinstance(mongo_obj['find'], dict):
        raise ValueError("Invalid MongoDB query: missing or invalid 'find' key.")

    table = mongo_obj.get("collection", "unknown_table")
    find_filter = mongo_obj.get("find", {})
    projection = mongo_obj.get("projection", {})
    sort_clause = mongo_obj.get("sort", [])  # e.g. [("field", 1), ("other", -1)]
    limit_val = mongo_obj.get("limit", None)
    skip_val = mongo_obj.get("skip", None)

    # Validate projection if provided
    if 'projection' in mongo_obj:
        if not isinstance(projection, dict) or any(val not in (0, 1) for val in projection.values()):
            raise ValueError("Invalid 'projection'; must be a dict mapping fields to 0 or 1.")

    # 1) Build the column list from projection
    columns = "*"
    if isinstance(projection, dict) and len(projection) > 0:
        # e.g. { "age":1, "status":1 }
        col_list = []
        for field, include in projection.items():
            if include == 1:
                col_list.append(field)
        if col_list:
            columns = ", ".join(col_list)

    # Validate sort clause if provided
    if 'sort' in mongo_obj:
        if not isinstance(sort_clause, list) or any(
            not (isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[0], str) and item[1] in (1, -1))
            for item in sort_clause
        ):
            raise ValueError("Invalid 'sort'; must be a list of (field, 1 or -1) tuples.")

    # 2) Build WHERE from find_filter
    where_sql = build_where_sql(find_filter)

    # Validate limit and skip
    if 'limit' in mongo_obj and limit_val is not None and not isinstance(limit_val, int):
        raise ValueError("Invalid 'limit'; must be an integer.")
    if 'skip' in mongo_obj and skip_val is not None and not isinstance(skip_val, int):
        raise ValueError("Invalid 'skip'; must be an integer.")

    # 3) Build ORDER BY from sort
    order_sql = build_order_by_sql(sort_clause)

    # 4) Combine everything
    sql = f"SELECT {columns} FROM {table}"

    if where_sql:
        sql += f" WHERE {where_sql}"

    if order_sql:
        sql += f" ORDER BY {order_sql}"

    # 5) Limit + Skip
    # skip in Mongo ~ "OFFSET" in SQL
    if isinstance(limit_val, int) and limit_val > 0:
        sql += f" LIMIT {limit_val}"
        if isinstance(skip_val, int) and skip_val > 0:
            sql += f" OFFSET {skip_val}"
    else:
        # If no limit but skip is provided, you can handle or ignore
        if isinstance(skip_val, int) and skip_val > 0:
            # Some SQL dialects allow "OFFSET" without a limit, others do not
            sql += f" LIMIT 999999999 OFFSET {skip_val}"

    sql += ";"
    return sql


def build_where_sql(find_filter) -> str:
    """
    Convert a 'find' dict into a SQL condition string.
    Supports:
      - direct equality: {field: value}
      - comparison operators: {field: {"$gt": val, ...}}
      - $in / $nin
      - $regex => LIKE
      - $and / $or => combine subclauses

    :param find_filter: The 'find' dict from MongoDB.
    :return: The SQL WHERE clause as a string.
    """
    if not find_filter:
        return ""

    # If top-level is a dictionary with $and / $or
    if isinstance(find_filter, dict):
        # check for $and / $or in the top-level
        if "$and" in find_filter:
            conditions = [build_where_sql(sub) for sub in find_filter["$and"]]
            # e.g. (cond1) AND (cond2)
            return "(" + ") AND (".join(cond for cond in conditions if cond) + ")"
        elif "$or" in find_filter:
            conditions = [build_where_sql(sub) for sub in find_filter["$or"]]
            return "(" + ") OR (".join(cond for cond in conditions if cond) + ")"
        else:
            # parse normal fields
            return build_basic_conditions(find_filter)

    # If top-level is a list => not typical, handle or skip
    if isinstance(find_filter, list):
        # e.g. $or array
        # but typically you'd see it as { "$or": [ {}, {} ] }
        subclauses = [build_where_sql(sub) for sub in find_filter]
        return "(" + ") AND (".join(sc for sc in subclauses if sc) + ")"

    # fallback: if it's a scalar or something unexpected
    return ""


def build_basic_conditions(condition_dict: dict) -> str:
    """
    For each field in condition_dict:
      if it's a direct scalar => field = value
      if it's an operator dict => interpret $gt, $in, etc.
    Return "field1 = val1 AND field2 >= val2" etc. combined.

    :param condition_dict: A dictionary of conditions.
    :return: A SQL condition string.
    """
    clauses = []
    for field, expr in condition_dict.items():
        # e.g. field => "status", expr => "ACTIVE"
        if isinstance(expr, dict):
            # parse operator e.g. {"$gt": 30}
            for op, val in expr.items():
                clause = convert_operator(field, op, val)
                if clause:
                    clauses.append(clause)
        else:
            # direct equality
            if isinstance(expr, (int, float)):
                clauses.append(f"{field} = {expr}")
            else:
                clauses.append(f"{field} = '{escape_quotes(str(expr))}'")

    return " AND ".join(clauses)


def convert_operator(field: str, op: str, val):
    """
    Handle operators like $gt, $in, $regex, etc.

    :param field: The field name.
    :param op: The operator (e.g., "$gt", "$in").
    """
    # Convert val to string with quotes if needed
    if isinstance(val, (int, float)):
        val_str = str(val)
    elif isinstance(val, list):
        # handle lists for $in, $nin
        val_str = ", ".join(quote_if_needed(item) for item in val)
    else:
        # string or other
        val_str = f"'{escape_quotes(str(val))}'"

    op_map = {
        "$gt": ">",
        "$gte": ">=",
        "$lt": "<",
        "$lte": "<=",
        "$eq": "=",
        "$ne": "<>",
        "$regex": "LIKE"
    }

    if op in op_map:
        sql_op = op_map[op]
        # e.g. "field > 30" or "field LIKE '%abc%'"
        return f"{field} {sql_op} {val_str}"
    elif op == "$in":
        # e.g. field IN (1,2,3)
        return f"{field} IN ({val_str})"
    elif op == "$nin":
        return f"{field} NOT IN ({val_str})"
    else:
        # fallback
        return f"{field} /*unknown op {op}*/ {val_str}"


def build_order_by_sql(sort_list):
    """
    If we have "sort": [("age", 1), ("name", -1)],
    => "age ASC, name DESC"

    :param sort_list: List of tuples (field, direction)
    :return: SQL ORDER BY clause as a string.
    """
    if not sort_list or not isinstance(sort_list, list):
        return ""
    order_parts = []
    for field_dir in sort_list:
        if isinstance(field_dir, tuple) and len(field_dir) == 2:
            field, direction = field_dir
            dir_sql = "ASC" if direction == 1 else "DESC"
            order_parts.append(f"{field} {dir_sql}")
    return ", ".join(order_parts)


def quote_if_needed(val):
    """
    Return a numeric or quoted string

    :param val: The value to quote if it's a string.
    :return: The value as a string, quoted if it's a string.
    """
    if isinstance(val, (int, float)):
        return str(val)
    return f"'{escape_quotes(str(val))}'"


def escape_quotes(s: str) -> str:
    """
    Simple approach to escape single quotes

    :param s: The string to escape.
    :return: The escaped string.
    """
    return s.replace("'", "''")


# --- JOIN PIPELINE HANDLER ---
def _handle_join_pipeline(mongo_obj: dict) -> str:
    """
    Convert a MongoDB aggregation pipeline with $lookup into a SQL JOIN query.
    Supports a single $lookup, optional $match, $project, $sort, and $limit stages.
    """
    table = mongo_obj.get("collection")
    pipeline = mongo_obj.get("pipeline", [])
    if not isinstance(pipeline, list) or not pipeline:
        raise ValueError("Invalid pipeline: must be a non-empty list of stages.")

    # Extract $lookup stage
    lookup = pipeline[0].get("$lookup") if isinstance(pipeline[0], dict) else None
    if not lookup:
        raise ValueError("First pipeline stage must be a $lookup for JOIN conversion.")
    joined = lookup.get("from")
    local = lookup.get("localField")
    foreign = lookup.get("foreignField")
    if not (joined and local and foreign):
        raise ValueError("$lookup must define 'from', 'localField', and 'foreignField'.")

    # Initialize SQL
    # Projection (fields)
    proj = next((stage.get('$project') for stage in pipeline if '$project' in stage), None)
    if proj:
        cols = []
        for key in proj:
            # Remove any aliasing if present
            if key.startswith(f"{joined}."):
                cols.append(key)
            else:
                cols.append(f"{table}.{key}")
        select_cols = ", ".join(cols)
    else:
        select_cols = f"{table}.*, {joined}.*"

    sql = f"SELECT {select_cols} FROM {table} "
    sql += f"JOIN {joined} ON {table}.{local} = {joined}.{foreign}"

    # WHERE from $match
    match_stage = next((stage.get('$match') for stage in pipeline if '$match' in stage), None)
    if match_stage:
        where_sql = build_where_sql(match_stage)
        if where_sql:
            sql += f" WHERE {where_sql}"

    # ORDER BY from $sort
    sort_stage = next((stage.get('$sort') for stage in pipeline if '$sort' in stage), None)
    if sort_stage and isinstance(sort_stage, dict):
        sort_list = [(f, d) for f, d in sort_stage.items()]
        order_sql = build_order_by_sql(sort_list)
        if order_sql:
            sql += f" ORDER BY {order_sql}"

    # LIMIT stage
    limit_stage = next((stage.get('$limit') for stage in pipeline if '$limit' in stage), None)
    if isinstance(limit_stage, int):
        sql += f" LIMIT {limit_stage}"

    return sql + ";"
