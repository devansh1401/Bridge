import sqlparse
from sqlparse.sql import (
    IdentifierList,
    Identifier,
    Where,
    Token,
    Parenthesis,
)
from sqlparse.tokens import Keyword, DML, Punctuation, Whitespace
import re
from typing import Dict, List, Tuple, Union, Optional, Any


def sql_to_mongo(sql_query: str) -> str:
    """
    Convert SQL statements (SELECT, INSERT, UPDATE, DELETE) to MongoDB commands.
    Returns the MongoDB command as a string ready to be executed.
    
    :param sql_query: SQL query as a string
    :return: MongoDB command as a string
    """
    # Disallow multiple statements
    parsed = sqlparse.parse(sql_query)
    if not parsed or len(parsed) != 1:
        raise SyntaxError("Please provide exactly one valid SQL statement.")
    
    statement = parsed[0]
    statement_type = statement.get_type().upper()
    
    # Route to appropriate handler based on statement type
    if statement_type == "SELECT":
        return _handle_select(statement, sql_query)
    elif statement_type == "INSERT":
        return _handle_insert(statement, sql_query)
    elif statement_type == "UPDATE":
        return _handle_update(statement, sql_query)
    elif statement_type == "DELETE":
        return _handle_delete(statement, sql_query)
    else:
        raise NotImplementedError(f"Statement type '{statement_type}' is not supported.")


def _handle_select(statement, sql_query: str) -> str:
    """
    Handle SELECT statements and convert to MongoDB find() or aggregate()
    
    :param statement: The parsed SQL statement
    :param sql_query: Original SQL query string
    :return: MongoDB command as a string
    """
    # Check for JOIN
    if re.search(r"\bJOIN\b", sql_query, re.IGNORECASE):
        result = _handle_join_query(sql_query)
        return _format_mongo_aggregate(result["collection"], result["pipeline"])
    
    # Validate comma-separated columns
    mcols = re.search(r"SELECT\s+(.*?)\s+FROM", sql_query, re.IGNORECASE | re.DOTALL)
    if mcols:
        cols_txt = mcols.group(1).strip()
        # ignore wildcard and single column
        if cols_txt and cols_txt != '*' and ',' not in cols_txt and len(cols_txt.split()) > 1:
            raise SyntaxError("Columns in SELECT must be comma-separated.")
    
    cols, table, where, order, group, limit = parse_select_statement(statement)
    
    # Ensure columns are specified (or wildcard)
    if not cols or all(c == '' for c in cols):
        raise SyntaxError("No columns specified in SELECT clause.")
    if not table:
        raise ValueError("Table name could not be determined from SQL query.")
    
    mongo_query = build_mongo_query(table, cols, where, order, group, limit)
    
    # If we have a GROUP BY clause, it needs to be handled as an aggregation
    if group:
        return _format_mongo_aggregate(mongo_query["collection"], [mongo_query["group"]])
    
    # Format the query as db.<collection>.find()
    return _format_mongo_find(mongo_query)


def _handle_insert(statement, sql_query: str) -> str:
    """
    Handle INSERT statements and convert to MongoDB insertOne() or insertMany()
    
    :param statement: The parsed SQL statement
    :param sql_query: Original SQL query string
    :return: MongoDB command as a string
    """
    # Match: INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)
    single_insert_pattern = re.compile(
        r"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)",
        re.IGNORECASE
    )
    
    # Match: INSERT INTO table_name VALUES (val1, val2, ...)
    simple_insert_pattern = re.compile(
        r"INSERT\s+INTO\s+(\w+)\s*VALUES\s*\(([^)]+)\)",
        re.IGNORECASE
    )
    
    # Match: INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...), (val1, val2, ...)
    multi_insert_pattern = re.compile(
        r"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(\((?:[^)(]+|\([^)(]*\))*\)(?:\s*,\s*\((?:[^)(]+|\([^)(]*\))*\))*)",
        re.IGNORECASE
    )
    
    # Try to match with columns first
    match = single_insert_pattern.search(sql_query)
    multi_match = multi_insert_pattern.search(sql_query)
    simple_match = None
    
    if multi_match:
        # Handle multi-row insert
        table_name = multi_match.group(1)
        columns = [col.strip() for col in multi_match.group(2).split(',')]
        values_block = multi_match.group(3)
        
        # Extract all value sets
        value_sets = re.findall(r'\(([^)]+)\)', values_block)
        documents = []
        
        for value_set in value_sets:
            values = [_parse_sql_value(val.strip()) for val in value_set.split(',')]
            if len(values) != len(columns):
                raise ValueError(f"Number of values ({len(values)}) doesn't match number of columns ({len(columns)})")
            
            document = dict(zip(columns, values))
            documents.append(document)
        
        if len(documents) == 1:
            return f"db.{table_name}.insertOne({_format_json(documents[0])})"
        else:
            return f"db.{table_name}.insertMany({_format_json(documents)})"
    
    elif match:
        # Handle single row insert with columns
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',')]
        values = [_parse_sql_value(val.strip()) for val in match.group(3).split(',')]
        
        if len(values) != len(columns):
            raise ValueError(f"Number of values ({len(values)}) doesn't match number of columns ({len(columns)})")
        
        document = dict(zip(columns, values))
        return f"db.{table_name}.insertOne({_format_json(document)})"
    
    else:
        # Try simple insert format
        simple_match = simple_insert_pattern.search(sql_query)
        if simple_match:
            table_name = simple_match.group(1)
            values = [_parse_sql_value(val.strip()) for val in simple_match.group(2).split(',')]
            
            # Without column names, we'll create a document with positional field names
            document = {f"field{i+1}": val for i, val in enumerate(values)}
            return f"db.{table_name}.insertOne({_format_json(document)})"
    
    raise SyntaxError("Invalid INSERT statement format")


def _handle_update(statement, sql_query: str) -> str:
    """
    Handle UPDATE statements and convert to MongoDB updateOne() or updateMany()
    
    :param statement: The parsed SQL statement
    :param sql_query: Original SQL query string
    :return: MongoDB command as a string
    """
    # Match: UPDATE table_name SET col1 = val1, col2 = val2 [WHERE condition]
    update_pattern = re.compile(
        r"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?(?:\s*;)?$",
        re.IGNORECASE | re.DOTALL
    )
    
    match = update_pattern.search(sql_query)
    if not match:
        raise SyntaxError("Invalid UPDATE statement format")
    
    table_name = match.group(1)
    set_clause = match.group(2).strip()
    where_clause = match.group(3)
    
    # Parse SET assignments
    set_items = {}
    for item in set_clause.split(','):
        if '=' not in item:
            raise SyntaxError(f"Invalid SET clause: {item}")
        
        field, value = item.split('=', 1)
        field = field.strip()
        value = _parse_sql_value(value.strip())
        set_items[field] = value
    
    # Parse WHERE conditions if present
    filter_query = {}
    if where_clause:
        filter_query = parse_where_conditions(where_clause)
    
    # Determine if updateOne or updateMany
    # For now, we'll use updateMany as the safer default
    update_command = "updateMany"
    
    return f"db.{table_name}.{update_command}({_format_json(filter_query)}, {{'$set': {_format_json(set_items)}}})"


def _handle_delete(statement, sql_query: str) -> str:
    """
    Handle DELETE statements and convert to MongoDB deleteOne() or deleteMany()
    
    :param statement: The parsed SQL statement
    :param sql_query: Original SQL query string
    :return: MongoDB command as a string
    """
    # Match: DELETE FROM table_name [WHERE condition]
    delete_pattern = re.compile(
        r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s*;)?$",
        re.IGNORECASE | re.DOTALL
    )
    
    match = delete_pattern.search(sql_query)
    if not match:
        raise SyntaxError("Invalid DELETE statement format")
    
    table_name = match.group(1)
    where_clause = match.group(2)
    
    # Parse WHERE conditions if present
    filter_query = {}
    if where_clause:
        filter_query = parse_where_conditions(where_clause)
    
    # Determine if deleteOne or deleteMany
    # For now, we'll use deleteMany as the safer default
    delete_command = "deleteMany"
    if filter_query and "_id" in filter_query:
        delete_command = "deleteOne"  # If specific ID, use deleteOne
    
    return f"db.{table_name}.{delete_command}({_format_json(filter_query)})"


def _format_mongo_find(query_obj: dict) -> str:
    """
    Format a MongoDB query object into a db.<collection>.find() style string
    
    :param query_obj: The MongoDB query object
    :return: A formatted MongoDB command string
    """
    collection = query_obj["collection"]
    find_filter = query_obj.get("find", {})
    projection = query_obj.get("projection")
    sort = query_obj.get("sort")
    limit = query_obj.get("limit")
    
    # Start with the basic find command
    cmd = f"db.{collection}.find({_format_json(find_filter)}"
    
    # Add projection if present
    if projection:
        cmd += f", {_format_json(projection)}"
    cmd += ")"
    
    # Add sort if present
    if sort:
        sort_obj = {field: direction for field, direction in sort}
        cmd += f".sort({_format_json(sort_obj)})"
    
    # Add limit if present
    if limit:
        cmd += f".limit({limit})"
    
    return cmd


def _format_mongo_aggregate(collection: str, pipeline: list) -> str:
    """
    Format a MongoDB aggregation pipeline into a db.<collection>.aggregate() style string
    
    :param collection: The collection name
    :param pipeline: The aggregation pipeline
    :return: A formatted MongoDB command string
    """
    return f"db.{collection}.aggregate({_format_json(pipeline)})"


def _format_json(obj: Any) -> str:
    """
    Convert a Python object to a MongoDB-style JSON string
    
    :param obj: The Python object to convert
    :return: A JSON string in MongoDB format
    """
    if isinstance(obj, dict):
        items = []
        for k, v in obj.items():
            items.append(f"{k}: {_format_json(v)}")
        return "{ " + ", ".join(items) + " }"
    elif isinstance(obj, list):
        items = [_format_json(item) for item in obj]
        return "[ " + ", ".join(items) + " ]"
    elif isinstance(obj, str):
        # Escape double quotes
        escaped = obj.replace('"', '\\"')
        return f'"{escaped}"'
    elif obj is None:
        return "null"
    else:
        return str(obj)


def _parse_sql_value(value: str) -> Any:
    """
    Parse SQL values into equivalent Python/MongoDB types
    
    :param value: The SQL value as a string
    :return: The parsed value
    """
    value = value.strip()
    
    # Handle string literals
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    
    # Handle NULL
    if value.upper() == "NULL":
        return None
    
    # Handle booleans
    if value.upper() == "TRUE":
        return True
    if value.upper() == "FALSE":
        return False
    
    # Handle numbers
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        # If we can't parse it as a number, return as is
        return value


def parse_select_statement(statement):
    """
    Parse:
      SELECT <columns> FROM <table>
      [WHERE ...]
      [GROUP BY ...]
      [ORDER BY ...]
      [LIMIT ...]
    in that approximate order.

    Returns:
      columns, table_name, where_clause_dict, order_by_list, group_by_list, limit_val

    :param statement: The parsed SQL statement.
    :return: A tuple containing columns, table_name, where_clause_dict, order_by_list, group_by_list, limit_val
    """
    columns = []
    table_name = None
    where_clause = {}
    order_by = []  # e.g. [("age", 1), ("name", -1)]
    group_by = []  # e.g. ["department", "role"]
    limit_val = None

    found_select = False
    reading_columns = False
    reading_from = False

    tokens = [t for t in statement.tokens if not t.is_whitespace]

    # We'll do multiple passes or a single pass with states
    # Single pass approach:
    i = 0
    while i < len(tokens):
        token = tokens[i]

        # detect SELECT
        if token.ttype is DML and token.value.upper() == "SELECT":
            found_select = True
            reading_columns = True
            i += 1
            continue

        # parse columns until we see FROM
        if reading_columns:
            if token.ttype is Keyword and token.value.upper() == "FROM":
                reading_columns = False
                reading_from = True
                i += 1
                continue
            else:
                possible_cols = extract_columns(token)
                if possible_cols:
                    columns = possible_cols
                i += 1
                continue

        # parse table name right after FROM
        if reading_from:
            # if token is Keyword (like WHERE, GROUP, ORDER), we skip
            if token.ttype is Keyword:
                # no table name found => might be incomplete
                reading_from = False
                # don't advance i, we'll handle logic below
            else:
                # assume table name
                table_name = str(token).strip()
                reading_from = False
            i += 1
            continue

        # check if token is a Where object => parse WHERE
        if isinstance(token, Where):
            where_clause = extract_where_clause(token)
            i += 1
            continue

        # or check if token is a simple 'WHERE' keyword
        if token.ttype is Keyword and token.value.upper() == "WHERE":
            # next token might be the actual conditions or a Where
            # try to gather the text
            # but often sqlparse lumps everything into a Where
            if i + 1 < len(tokens):
                next_token = tokens[i + 1]
                if isinstance(next_token, Where):
                    where_clause = extract_where_clause(next_token)
                    i += 2
                    continue
                else:
                    # fallback substring approach if needed
                    where_clause_text = str(next_token).strip()
                    where_clause = parse_where_conditions(where_clause_text)
                    i += 2
                    continue
            i += 1
            continue

        # handle ORDER BY
        if token.ttype is Keyword and token.value.upper() == "ORDER":
            # next token should be BY
            i += 1
            if i < len(tokens):
                nxt = tokens[i]
                if nxt.ttype is Keyword and nxt.value.upper() == "BY":
                    i += 1
                    # parse the next token as columns
                    if i < len(tokens):
                        order_by = parse_order_by(tokens[i])
                        i += 1
                        continue
            else:
                i += 1
                continue

        # handle GROUP BY
        if token.ttype is Keyword and token.value.upper() == "GROUP":
            # next token should be BY
            i += 1
            if i < len(tokens):
                nxt = tokens[i]
                if nxt.ttype is Keyword and nxt.value.upper() == "BY":
                    i += 1
                    # parse group by columns
                    if i < len(tokens):
                        group_by = parse_group_by(tokens[i])
                        i += 1
                        continue
            else:
                i += 1
                continue

        # handle LIMIT
        if token.ttype is Keyword and token.value.upper() == "LIMIT":
            # next token might be the limit number
            if i + 1 < len(tokens):
                limit_val = parse_limit_value(tokens[i + 1])
                i += 2
                continue

        i += 1

    return columns, table_name, where_clause, order_by, group_by, limit_val


def extract_columns(token):
    """
    If token is an IdentifierList => multiple columns
    If token is an Identifier => single column
    If token is '*' => wildcard

    Return a list of columns.
    If no columns found, return an empty list.

    :param token: The SQL token to extract columns from.
    :return: A list of columns.
    """
    from sqlparse.sql import IdentifierList, Identifier
    if isinstance(token, IdentifierList):
        return [str(ident).strip() for ident in token.get_identifiers()]
    elif isinstance(token, Identifier):
        return [str(token).strip()]
    else:
        raw = str(token).strip()
        raw = raw.replace(" ", "")
        if not raw:
            return []
        return [raw]


def extract_where_clause(where_token):
    """
    If where_token is a Where object => parse out 'WHERE' prefix, then parse conditions
    If where_token is a simple 'WHERE' keyword => parse conditions directly

    Return a dict of conditions.

    :param where_token: The SQL token to extract the WHERE clause from.
    :return: A dict of conditions.
    """
    raw = str(where_token).strip()
    if raw.upper().startswith("WHERE"):
        raw = raw[5:].strip()
    return parse_where_conditions(raw)


def parse_where_conditions(text: str):
    """
    e.g. "age > 30 AND name = 'Alice'"
    => { "age":{"$gt":30}, "name":"Alice" }
    We'll strip trailing semicolon as well.

    Supports:
        - direct equality: {field: value}
        - inequality: {field: {"$gt": value}}
        - other operators: {field: {"$op?": value}}

    :param text: The WHERE clause text.
    :return: A dict of conditions.
    """
    text = text.strip().rstrip(";")
    if not text:
        return {}

    # naive split on " AND "
    parts = text.split(" AND ")
    out = {}
    for part in parts:
        tokens = part.split(None, 2)  # e.g. ["age", ">", "30"]
        if len(tokens) < 3:
            continue
        field, op, val = tokens[0], tokens[1], tokens[2]
        val = val.strip().rstrip(";").strip("'").strip('"')
        if op == "=":
            out[field] = val
        elif op == ">":
            out[field] = {"$gt": convert_value(val)}
        elif op == "<":
            out[field] = {"$lt": convert_value(val)}
        elif op == ">=":
            out[field] = {"$gte": convert_value(val)}
        elif op == "<=":
            out[field] = {"$lte": convert_value(val)}
        else:
            out[field] = {"$op?": val}
    return out


def parse_order_by(token):
    """
    e.g. "age ASC, name DESC"
    Return [("age",1), ("name",-1)]

    :param token: The SQL token to extract the ORDER BY clause from.
    :return: A list of tuples (field, direction).
    """
    raw = str(token).strip().rstrip(";")
    if not raw:
        return []
    # might be multiple columns
    parts = raw.split(",")
    order_list = []
    for part in parts:
        sub = part.strip().split()
        if len(sub) == 1:
            # e.g. "age"
            order_list.append((sub[0], 1))  # default ASC
        elif len(sub) == 2:
            # e.g. "age ASC" or "name DESC"
            field, direction = sub[0], sub[1].upper()
            if direction == "ASC":
                order_list.append((field, 1))
            elif direction == "DESC":
                order_list.append((field, -1))
            else:
                order_list.append((field, 1))  # fallback
        else:
            # fallback
            order_list.append((part.strip(), 1))
    return order_list


def parse_group_by(token):
    """
    e.g. "department, role"
    => ["department", "role"]

    :param token: The SQL token to extract the GROUP BY clause from.
    :return: A list of columns.
    """
    raw = str(token).strip().rstrip(";")
    if not raw:
        return []
    return [x.strip() for x in raw.split(",")]


def parse_limit_value(token):
    """
    e.g. "100"
    => 100 (int)

    :param token: The SQL token to extract the LIMIT value from.
    :return: The LIMIT value as an integer, or None if not a valid integer.
    """
    raw = str(token).strip().rstrip(";")
    try:
        return int(raw)
    except ValueError:
        return None


def convert_value(val: str):
    """
    Convert a value to an int, float, or string.

    :param val: The value to convert.
    :return: The value as an int, float, or string.
    """
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val


def build_mongo_find(table_name, where_clause, columns):
    """
    Build a MongoDB find query.

    :param table_name: The name of the collection.
    :param where_clause: The WHERE clause as a dict.
    :param columns: The list of columns to select.
    :return: A dict representing the MongoDB find query.
    """
    filter_query = where_clause or {}
    projection = {}
    if columns and "*" not in columns:
        for col in columns:
            projection[col] = 1
    return {
        "collection": table_name,
        "find": filter_query,
        "projection": projection if projection else None
    }


def build_mongo_query(table_name, columns, where_clause, order_by, group_by, limit_val):
    """
    Build a MongoDB query object from parsed SQL components.

    We'll store everything in a single dict:
      {
        "collection": table_name,
        "find": {...},
        "projection": {...},
        "sort": [("col",1),("col2",-1)],
        "limit": int or None,
        "group": {...}
      }

    :param table_name: The name of the collection.
    :param columns: The list of columns to select.
    """
    query_obj = build_mongo_find(table_name, where_clause, columns)

    # Add sort
    if order_by:
        query_obj["sort"] = order_by

    # Add limit
    if limit_val is not None:
        query_obj["limit"] = limit_val

    # If group_by is used:
    if group_by:
        # e.g. group_by = ["department","role"]
        # We'll store a $group pipeline
        # Real logic depends on what columns are selected
        group_pipeline = {
            "$group": {
                "_id": {},
                "count": {"$sum": 1}
            }
        }
        # e.g. _id => { department: "$department", role: "$role" }
        _id_obj = {}
        for gb in group_by:
            _id_obj[gb] = f"${gb}"
        group_pipeline["$group"]["_id"] = _id_obj
        query_obj["group"] = group_pipeline
    return query_obj


# --- JOIN HANDLER ---
def _handle_join_query(sql_query: str) -> dict:
    """
    Handle a simple INNER JOIN query and convert it to a MongoDB aggregation pipeline.
    Supports one JOIN with a single ON= equality condition.
    """
    # Match SELECT ... FROM ... [alias] JOIN ... [alias] ON ...; with optional clauses
    pattern = re.compile(
        r"^SELECT\s+(?P<cols>.*?)\s+FROM\s+(?P<tbl1>\w+)(?:\s+(?P<alias1>\w+))?"
        r"\s+JOIN\s+(?P<tbl2>\w+)(?:\s+(?P<alias2>\w+))?"
        r"\s+ON\s+(?P<t1>\w+)\.(?P<c1>\w+)\s*=\s*(?P<t2>\w+)\.(?P<c2>\w+)"
        r"(?:\s+WHERE\s+(?P<where>.*?))?"
        r"(?:\s+ORDER\s+BY\s+(?P<order>.*?))?"
        r"(?:\s+LIMIT\s+(?P<limit>\d+))?"
        r"\s*;?$",
        re.IGNORECASE | re.DOTALL
    )
    m = pattern.match(sql_query.strip())
    if not m:
        raise SyntaxError("Unable to parse JOIN query.")
    cols_str = m.group("cols")
    tbl1 = m.group("tbl1")
    alias1 = m.group("alias1") or tbl1
    tbl2 = m.group("tbl2")
    alias2 = m.group("alias2") or tbl2
    t1 = m.group("t1")
    c1 = m.group("c1")
    t2 = m.group("t2")
    c2 = m.group("c2")
    where = m.group("where")
    order = m.group("order")
    limit = m.group("limit")

    # Parse columns list
    cols = [c.strip() for c in cols_str.split(",")]

    pipeline = []
    # $lookup stage (always map fields from actual table names)
    pipeline.append({
        "$lookup": {
            "from": tbl2,
            "localField": c1,
            "foreignField": c2,
            "as": alias2
        }
    })
    # $unwind the joined array
    pipeline.append({"$unwind": f"${alias2}"})
    # Optional $match stage
    if where:
        pipeline.append({"$match": parse_where_conditions(where)})
    # Optional $project stage
    if cols and cols != ["*"]:
        proj = {}
        for col in cols:
            if "." in col:
                t, fld = col.split(".", 1)
                if t == alias1:
                    proj[f"{tbl1}.{fld}"] = 1
                else:
                    proj[f"{tbl2}.{fld}"] = 1
            else:
                proj[f"{tbl1}.{col}"] = 1
        pipeline.append({"$project": proj})
    # Optional $sort stage
    if order:
        order_list = parse_order_by(order)
        sort_dict = {f: d for f, d in order_list}
        pipeline.append({"$sort": sort_dict})
    # Optional $limit stage
    if limit:
        pipeline.append({"$limit": int(limit)})

    return {"collection": tbl1, "pipeline": pipeline}


# For backward compatibility
def sql_select_to_mongo(sql_query: str) -> dict:
    """
    Legacy function to maintain backward compatibility.
    Convert a SELECT SQL query to a MongoDB query dict.
    
    :param sql_query: The SQL query as a string
    :return: A MongoDB query dict
    """
    # Parse the SQL query
    parsed = sqlparse.parse(sql_query)
    if not parsed or len(parsed) != 1:
        raise SyntaxError("Please provide exactly one valid SQL statement.")
    
    statement = parsed[0]
    if statement.get_type().upper() != "SELECT":
        raise NotImplementedError("Only SELECT statements are supported by this function.")
    
    # If JOIN, use specialized handler
    if re.search(r"\bJOIN\b", sql_query, re.IGNORECASE):
        return _handle_join_query(sql_query)
    
    # Validate comma-separated columns
    mcols = re.search(r"SELECT\s+(.*?)\s+FROM", sql_query, re.IGNORECASE | re.DOTALL)
    if mcols:
        cols_txt = mcols.group(1).strip()
        if cols_txt and cols_txt != '*' and ',' not in cols_txt and len(cols_txt.split()) > 1:
            raise SyntaxError("Columns in SELECT must be comma-separated.")
    
    cols, table, where, order, group, limit = parse_select_statement(statement)
    
    if not cols or all(c == '' for c in cols):
        raise SyntaxError("No columns specified in SELECT clause.")
    if not table:
        raise ValueError("Table name could not be determined from SQL query.")
    
    return build_mongo_query(table, cols, where, order, group, limit)