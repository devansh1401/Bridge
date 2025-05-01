import sqlparse
from sqlparse.sql import (
    IdentifierList,
    Identifier,
    Where,
    Token,
    Parenthesis,
)
from sqlparse.tokens import Keyword, DML
import re


def sql_select_to_mongo(sql_query: str) -> dict:
    """
    Convert a single SELECT...FROM...WHERE...ORDER BY...GROUP BY...LIMIT...
    SQL statement into a MongoDB query dict.
    """
    # Validate comma-separated columns: if multiple words in SELECT list but no comma, error
    mcols = re.search(r"SELECT\s+(.*?)\s+FROM", sql_query, re.IGNORECASE | re.DOTALL)
    if mcols:
        cols_txt = mcols.group(1).strip()
        # ignore wildcard and single column
        if cols_txt and cols_txt != '*' and ',' not in cols_txt and len(cols_txt.split()) > 1:
            raise SyntaxError("Columns in SELECT must be comma-separated.")
    # If there's a JOIN, use the specialized join handler
    if re.search(r"\bJOIN\b", sql_query, re.IGNORECASE):
        return _handle_join_query(sql_query)
    """
    Convert a SELECT...FROM...WHERE...ORDER BY...GROUP BY...LIMIT...
    into a Mongo dict:

    {
      "collection": <table>,
      "find": { ...where... },
      "projection": { col1:1, col2:1 } or None,
      "sort": [...],
      "limit": int,
      "group": { ... }
    }

    :param sql_query: The SQL SELECT query as a string.
    :return: A naive MongoDB find dict.
    """
    # Disallow multiple statements
    parsed = sqlparse.parse(sql_query)
    if not parsed or len(parsed) != 1:
        raise SyntaxError("Please provide exactly one valid SQL statement.")
    statement = parsed[0]
    if statement.get_type().upper() != "SELECT":
        raise NotImplementedError("Only SELECT statements are supported.")

    cols, table, where, order, group, limit = parse_select_statement(statement)
    # Ensure columns are specified (or wildcard)
    if not cols or all(c == '' for c in cols):
        raise SyntaxError("No columns specified in SELECT clause.")
    if not table:
        raise ValueError("Table name could not be determined from SQL query.")

    return build_mongo_query(table, cols, where, order, group, limit)


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
