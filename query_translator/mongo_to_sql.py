import re
from typing import Dict, List, Union, Tuple, Any, Optional


class MongoToSql:
    """
    Converter for MongoDB queries to SQL statements.
    Supports all CRUD operations:
    - Create (insert)
    - Read (find/aggregate)
    - Update (update/updateMany)
    - Delete (deleteOne/deleteMany)
    
    Also supports MongoDB shell syntax like db.collection.find({...})
    """
    
    def __init__(self):
        # Regex patterns for parsing MongoDB shell syntax
        # In MongoToSql.__init__
        self.find_pattern = r'db\.(\w+)\.find\(([\s\S]*?)\)(?:\.sort\(([\s\S]*?)\))?(?:\.limit\((\d+)\))?(?:\.skip\((\d+)\))?'
        self.insert_pattern = r'db\.(\w+)\.insert(?:One|Many)?\((.*)\)'
        self.update_pattern = r'db\.(\w+)\.update(?:One|Many)?\((.*?),\s*(.*?)(?:,\s*({.*?}))?\)'
        self.delete_pattern = r'db\.(\w+)\.delete(?:One|Many)?\((.*?)(?:,\s*({.*?}))?\)'
        self.aggregate_pattern = r'db\.(\w+)\.aggregate\((.*)\)'
    
    def convert(self, mongo_query: str) -> str:
        """
        Main entry point to convert a MongoDB query string to SQL.
        Enhanced to better handle complex queries.
        
        :param mongo_query: MongoDB query in shell syntax like db.collection.find({...})
        :return: SQL statement
        """
        mongo_query = mongo_query.strip()
            
        # Determine the operation type and delegate to appropriate handler
        if re.search(r'\.find\(', mongo_query):
            return self._handle_find(mongo_query)
        elif re.search(r'\.insert(?:One|Many)?\(', mongo_query):
            return self._handle_insert(mongo_query)
        elif re.search(r'\.update(?:One|Many)?\(', mongo_query):
            return self._handle_update(mongo_query)
        elif re.search(r'\.delete(?:One|Many)?\(', mongo_query):
            return self._handle_delete(mongo_query)
        elif re.search(r'\.aggregate\(', mongo_query):
            return self._handle_aggregate(mongo_query)
        else:
            raise ValueError(f"Unsupported MongoDB operation in: {mongo_query}")

    def _parse_mongo_json(self, js_obj_str: str) -> Dict:
        """
        Parse MongoDB-style JSON objects with improved handling for complex structures.
        
        :param js_obj_str: String containing MongoDB-style JSON object
        :return: Python dictionary representation
        """
        js_obj_str = js_obj_str.strip()
        
        # If empty, return empty dict
        if not js_obj_str or js_obj_str == '{}':
            return {}
        
        # Special handling for MongoDB shell syntax:
        
        # 1. First handle the case where 'gt' is used without the '$' prefix
        # Look for MongoDB operators without $ and add the $
        operator_patterns = [
            (r'(\s*)gt(\s*):',  r'\1"$gt"\2:'),
            (r'(\s*)gte(\s*):', r'\1"$gte"\2:'),
            (r'(\s*)lt(\s*):',  r'\1"$lt"\2:'),
            (r'(\s*)lte(\s*):', r'\1"$lte"\2:'),
            (r'(\s*)eq(\s*):',  r'\1"$eq"\2:'),
            (r'(\s*)ne(\s*):',  r'\1"$ne"\2:'),
            (r'(\s*)in(\s*):',  r'\1"$in"\2:'),
            (r'(\s*)nin(\s*):', r'\1"$nin"\2:'),
            (r'(\s*)or(\s*):',  r'\1"$or"\2:'),
            (r'(\s*)and(\s*):', r'\1"$and"\2:'),
        ]
        
        for pattern, replacement in operator_patterns:
            js_obj_str = re.sub(pattern, replacement, js_obj_str)
        
        # 2. Quote all remaining unquoted keys at all levels
        # This is more complex - we need to handle nested objects
        def quote_keys(match):
            return f'"{match.group(1)}":'
        
        # Look for word characters followed by a colon (but not already quoted)
        js_obj_str = re.sub(r'(?<!")(\w+)(?=\s*:)', quote_keys, js_obj_str)
        
        # 3. Handle MongoDB operators with $ prefix
        for op in ['$gt', '$gte', '$lt', '$lte', '$eq', '$ne', '$in', '$nin', '$regex', '$exists', '$or', '$and']:
            # Make sure the operator is properly quoted
            js_obj_str = js_obj_str.replace(f'"{op}"', f'"{op}"')
            # Also handle the case where $ might not be in quotes
            js_obj_str = js_obj_str.replace(f'{op}:', f'"{op}":')
        
        try:
            # Try json.loads first as it's safer
            import json
            try:
                return json.loads(js_obj_str)
            except json.JSONDecodeError:
                # If that fails, try ast.literal_eval as a fallback
                import ast
                # Convert JS booleans/null to Python
                js_obj_str = js_obj_str.replace('true', 'True').replace('false', 'False').replace('null', 'None')
                return ast.literal_eval(js_obj_str)
        except Exception as e:
            # If all parsing attempts fail, provide a helpful error message
            raise ValueError(f"Error parsing MongoDB query object: {js_obj_str}\nError: {str(e)}")
            
    def _safe_eval(self, js_obj_str: str) -> Dict:
        """
        Backward compatibility method - redirects to _parse_mongo_json
        """
        return self._parse_mongo_json(js_obj_str)
    
    def _balance_brackets(self, js_str: str) -> str:
        """
        Balance brackets in a potentially unbalanced JSON string.
        
        :param js_str: The potentially unbalanced JSON string
        :return: A balanced JSON string
        """
        # Count different types of brackets
        open_curly = js_str.count('{')
        close_curly = js_str.count('}')
        open_square = js_str.count('[')
        close_square = js_str.count(']')
        
        # Add missing closing brackets
        if open_curly > close_curly:
            js_str += '}' * (open_curly - close_curly)
        if open_square > close_square:
            js_str += ']' * (open_square - close_square)
            
        return js_str
        
    def _parse_or_operator(self, js_str: str) -> Dict:
        """
        Special handler for $or operator queries.
        
        :param js_str: The MongoDB query string with $or operator
        :return: Parsed dictionary
        """
        # Simple implementation for the common case
        if '$or' in js_str and '[' in js_str:
            # Find the conditions within the $or array
            try:
                # Extract content between [ and ]
                start_idx = js_str.find('[')
                end_idx = js_str.rfind(']')
                
                if start_idx > 0 and end_idx > start_idx:
                    conditions_str = js_str[start_idx+1:end_idx]
                    
                    # Split the conditions by },{
                    raw_conditions = conditions_str.split('},{')
                    conditions = []
                    
                    for i, cond in enumerate(raw_conditions):
                        # Add missing braces
                        if not cond.startswith('{'):
                            cond = '{' + cond
                        if not cond.endswith('}'):
                            cond = cond + '}'
                        
                        # Parse the condition
                        try:
                            parsed_cond = eval(cond, {"__builtins__": {}})
                            conditions.append(parsed_cond)
                        except:
                            # Skip invalid conditions
                            pass
                    
                    return {"$or": conditions}
            except:
                pass
                
        # Fallback to the original string if we can't parse it
        raise ValueError(f"Could not parse $or operator in: {js_str}")

    def _extract_balanced_json(self, js_str: str) -> str:
        """
        Attempt to extract a balanced JSON object from a potentially unbalanced string.
        
        :param js_str: The potentially unbalanced JSON string
        :return: A balanced JSON string if possible, otherwise the original string
        """
        # Simple stack-based bracket balancing
        stack = []
        brackets = {'(': ')', '[': ']', '{': '}'}
        
        # Try to find the actual ending of the JSON object
        for i, char in enumerate(js_str):
            if char in brackets.keys():
                stack.append(char)
            elif char in brackets.values():
                # Check if this closing bracket matches the last opening bracket
                if not stack or char != brackets.get(stack[-1], None):
                    # Unmatched closing bracket
                    return js_str  # Return original if we can't easily fix it
                stack.pop()
                
                # If stack is empty, we've found a balanced substring
                if not stack and i > 0:
                    return js_str[:i+1]
        
        return js_str
    

    def _find_matching_bracket(self, text: str, open_pos: int) -> int:
        """
        Find the matching closing bracket for an opening bracket at the given position.
        
        :param text: The text containing brackets
        :param open_pos: Position of the opening bracket
        :return: Position of the matching closing bracket, or -1 if not found
        """
        if open_pos >= len(text) or text[open_pos] not in '({[':
            return -1
            
        open_char = text[open_pos]
        close_char = {'(': ')', '{': '}', '[': ']'}[open_char]
        stack = 1  # Start with 1 for the opening bracket we're already on
        
        for i in range(open_pos + 1, len(text)):
            if text[i] == open_char:
                stack += 1
            elif text[i] == close_char:
                stack -= 1
                if stack == 0:
                    return i  # Found matching bracket
        
        return -1  # No matching bracket found
    
    def _handle_find(self, mongo_query: str) -> str:
        """
        Handle MongoDB find() queries and convert to SQL SELECT.
        Enhanced to better handle complex queries.
        """
        # Try to parse the query using our regex pattern
        match = re.search(self.find_pattern, mongo_query)
        if not match:
            raise ValueError(f"Invalid MongoDB find query format: {mongo_query}")
        
        collection = match.group(1)
        query_params = match.group(2).strip()
        
        # Split the query parameters into filter and projection parts while respecting nested structures
        params = self._split_respecting_brackets(query_params)
        query_filter_str = params[0] if params else '{}'
        projection_str = params[1] if len(params) > 1 else '{}'
        
        try:
            # Parse query filter with improved handling for complex structures
            query_filter = self._parse_mongo_json(query_filter_str)
            projection = self._parse_mongo_json(projection_str)
            
            # Handle sort, limit, skip if present
            sort_str = match.group(3)
            limit_str = match.group(4)
            skip_str = match.group(5)
            
            sort_clause = []
            if sort_str:
                sort_obj = self._parse_mongo_json(sort_str)
                sort_clause = [(field, direction) for field, direction in sort_obj.items()]
            
            limit_val = int(limit_str) if limit_str else None
            skip_val = int(skip_str) if skip_str else None
            
            mongo_obj = {
                "collection": collection,
                "find": query_filter,
                "projection": projection
            }
            
            if sort_clause:
                mongo_obj["sort"] = sort_clause
            if limit_val is not None:
                mongo_obj["limit"] = limit_val
            if skip_val is not None:
                mongo_obj["skip"] = skip_val
                
            return self._mongo_find_to_sql(mongo_obj)
        except Exception as e:
            raise ValueError(f"Error parsing MongoDB query: {mongo_query}\nError: {str(e)}")
    
    def _split_respecting_brackets(self, text: str) -> List[str]:
        """
        Split a string by commas, but respect nested brackets.
        
        :param text: The text to split
        :return: List of split parts
        """
        parts = []
        current_part = ""
        bracket_count = 0
        
        for char in text:
            if char in '{[(':
                bracket_count += 1
            elif char in '}])':
                bracket_count -= 1
            
            if char == ',' and bracket_count == 0:
                parts.append(current_part.strip())
                current_part = ""
            else:
                current_part += char
        
        if current_part:
            parts.append(current_part.strip())
        
        return parts

    def _handle_insert(self, mongo_query: str) -> str:
        """
        Handle MongoDB insertOne/insertMany queries and convert to SQL INSERT
        
        :param mongo_query: MongoDB insert query string
        :return: SQL INSERT statement
        """
        match = re.search(self.insert_pattern, mongo_query)
        if not match:
            raise ValueError(f"Invalid MongoDB insert query format: {mongo_query}")
        
        collection = match.group(1)
        docs_str = match.group(2)
        
        # Parse the document(s) to insert
        docs = self._safe_eval(docs_str)
        
        # Handle both single document and array of documents
        if isinstance(docs, dict):
            docs = [docs]
        
        if not docs:
            raise ValueError("No documents to insert")
        
        # Get all fields from all documents
        all_fields = set()
        for doc in docs:
            all_fields.update(doc.keys())
        
        # Convert to SQL INSERT
        fields_str = ", ".join(all_fields)
        values_list = []
        
        for doc in docs:
            values = []
            for field in all_fields:
                if field in doc:
                    val = doc[field]
                    if isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        values.append(f"'{self._escape_quotes(str(val))}'")
                else:
                    values.append("NULL")
            values_list.append(f"({', '.join(values)})")
        
        values_str = ", ".join(values_list)
        
        return f"INSERT INTO {collection} ({fields_str}) VALUES {values_str};"

    def _handle_update(self, mongo_query: str) -> str:
        """
        Handle MongoDB updateOne/updateMany queries and convert to SQL UPDATE
        
        :param mongo_query: MongoDB update query string
        :return: SQL UPDATE statement
        """
        match = re.search(self.update_pattern, mongo_query)
        if not match:
            raise ValueError(f"Invalid MongoDB update query format: {mongo_query}")
        
        collection = match.group(1)
        filter_str = match.group(2)
        update_str = match.group(3)
        options_str = match.group(4) if match.group(4) else '{}'
        
        filter_obj = self._safe_eval(filter_str)
        update_obj = self._safe_eval(update_str)
        options = self._safe_eval(options_str)
        
        # Build WHERE clause from filter
        where_clause = self._build_where_sql(filter_obj)
        
        # Extract $set operator - this is what we'll use for SQL SET clause
        if '$set' in update_obj:
            set_obj = update_obj['$set']
        else:
            # If no $set, assume the update object itself contains the fields to set
            set_obj = update_obj
        
        # Build SET clause
        set_clauses = []
        for field, value in set_obj.items():
            if isinstance(value, (int, float)):
                set_clauses.append(f"{field} = {value}")
            else:
                set_clauses.append(f"{field} = '{self._escape_quotes(str(value))}'")
        
        set_clause = ", ".join(set_clauses)
        
        sql = f"UPDATE {collection} SET {set_clause}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        
        # Add LIMIT 1 for updateOne
        if 'updateOne' in mongo_query:
            sql += " LIMIT 1"
            
        return sql + ";"

    def _handle_delete(self, mongo_query: str) -> str:
        """
        Handle MongoDB deleteOne/deleteMany queries and convert to SQL DELETE
        
        :param mongo_query: MongoDB delete query string
        :return: SQL DELETE statement
        """
        match = re.search(self.delete_pattern, mongo_query)
        if not match:
            raise ValueError(f"Invalid MongoDB delete query format: {mongo_query}")
        
        collection = match.group(1)
        filter_str = match.group(2)
        options_str = match.group(3) if match.group(3) else '{}'
        
        filter_obj = self._safe_eval(filter_str)
        options = self._safe_eval(options_str)
        
        # Build WHERE clause from filter
        where_clause = self._build_where_sql(filter_obj)
        
        sql = f"DELETE FROM {collection}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        
        # Add LIMIT 1 for deleteOne
        if 'deleteOne' in mongo_query:
            sql += " LIMIT 1"
            
        return sql + ";"

    def _handle_aggregate(self, mongo_query: str) -> str:
        """
        Handle MongoDB aggregate pipeline and convert to SQL
        
        :param mongo_query: MongoDB aggregate query string
        :return: SQL statement
        """
        match = re.search(self.aggregate_pattern, mongo_query)
        if not match:
            raise ValueError(f"Invalid MongoDB aggregate query format: {mongo_query}")
        
        collection = match.group(1)
        pipeline_str = match.group(2)
        
        # Parse the pipeline stages
        pipeline = self._safe_eval(pipeline_str)
        
        # Create mongo_obj for compatibility with existing code
        mongo_obj = {
            "collection": collection,
            "pipeline": pipeline
        }
        
        # If pipeline starts with $match, extract it as find filter
        if pipeline and '$match' in pipeline[0]:
            mongo_obj["find"] = pipeline[0]['$match']
        
        # Check for $group stage
        group_stage = next((stage for stage in pipeline if '$group' in stage), None)
        if group_stage:
            mongo_obj["group"] = group_stage
        
        # Check for $lookup stage (JOIN)
        lookup_stage = next((stage for stage in pipeline if '$lookup' in stage), None)
        if lookup_stage and lookup_stage == pipeline[0]:  # Our code only handles $lookup as first stage
            return self._handle_join_pipeline(mongo_obj)
        
        # If we have a group, handle it
        if "group" in mongo_obj:
            sql = self._mongo_find_to_sql(mongo_obj)
            return sql
        
        # Otherwise, handle as a regular find with pipeline stages
        # Extract relevant stages like $match, $project, $sort, $limit
        for stage in pipeline:
            if '$match' in stage:
                mongo_obj["find"] = stage['$match']
            elif '$project' in stage:
                mongo_obj["projection"] = stage['$project']
            elif '$sort' in stage:
                sort_obj = stage['$sort']
                mongo_obj["sort"] = [(field, direction) for field, direction in sort_obj.items()]
            elif '$limit' in stage:
                mongo_obj["limit"] = stage['$limit']
            elif '$skip' in stage:
                mongo_obj["skip"] = stage['$skip']
        
        return self._mongo_find_to_sql(mongo_obj)

    def _mongo_find_to_sql(self, mongo_obj: dict) -> str:
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
            where_sql = self._build_where_sql(mongo_obj.get('find', {}))
            if where_sql:
                sql += f" WHERE {where_sql}"
            order_sql = self._build_order_by_sql(mongo_obj.get('sort', []))
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
            return self._handle_join_pipeline(mongo_obj)

        # Validate input structure
        if not isinstance(mongo_obj, dict):
            raise TypeError("Input must be a dict representing a MongoDB query.")
        if 'collection' not in mongo_obj or not isinstance(mongo_obj['collection'], str):
            raise ValueError("Invalid MongoDB query: missing or invalid 'collection' key.")

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
            if not isinstance(sort_clause, list) and not isinstance(sort_clause, dict):
                raise ValueError("Invalid 'sort'; must be a list of (field, direction) tuples or a dict.")
            
            # If sort_clause is a dict, convert to list of tuples
            if isinstance(sort_clause, dict):
                sort_clause = [(field, direction) for field, direction in sort_clause.items()]

        # 2) Build WHERE from find_filter
        where_sql = self._build_where_sql(find_filter)

        # Validate limit and skip
        if 'limit' in mongo_obj and limit_val is not None and not isinstance(limit_val, int):
            raise ValueError("Invalid 'limit'; must be an integer.")
        if 'skip' in mongo_obj and skip_val is not None and not isinstance(skip_val, int):
            raise ValueError("Invalid 'skip'; must be an integer.")

        # 3) Build ORDER BY from sort
        order_sql = self._build_order_by_sql(sort_clause)

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
                sql += f" LIMIT 18446744073709551615 OFFSET {skip_val}"  # MySQL max BIGINT UNSIGNED

        sql += ";"
        return sql

    def _build_where_sql(self, find_filter) -> str:
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
                conditions = [self._build_where_sql(sub) for sub in find_filter["$and"]]
                # e.g. (cond1) AND (cond2)
                return "(" + ") AND (".join(cond for cond in conditions if cond) + ")"
            elif "$or" in find_filter:
                conditions = [self._build_where_sql(sub) for sub in find_filter["$or"]]
                return "(" + ") OR (".join(cond for cond in conditions if cond) + ")"
            else:
                # parse normal fields
                return self._build_basic_conditions(find_filter)

        # If top-level is a list => not typical, handle or skip
        if isinstance(find_filter, list):
            # e.g. $or array
            # but typically you'd see it as { "$or": [ {}, {} ] }
            subclauses = [self._build_where_sql(sub) for sub in find_filter]
            return "(" + ") AND (".join(sc for sc in subclauses if sc) + ")"

        # fallback: if it's a scalar or something unexpected
        return ""

    def _build_basic_conditions(self, condition_dict: dict) -> str:
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
                    clause = self._convert_operator(field, op, val)
                    if clause:
                        clauses.append(clause)
            else:
                # direct equality
                if isinstance(expr, (int, float)):
                    clauses.append(f"{field} = {expr}")
                elif expr is None:
                    clauses.append(f"{field} IS NULL")
                else:
                    clauses.append(f"{field} = '{self._escape_quotes(str(expr))}'")

        return " AND ".join(clauses)

    def _convert_operator(self, field: str, op: str, val):
        """
        Handle operators like $gt, $in, $regex, etc.

        :param field: The field name.
        :param op: The operator (e.g., "$gt", "$in").
        :param val: The value to compare against.
        :return: SQL condition string
        """
        # Convert val to string with quotes if needed
        if val is None:
            if op == "$eq":
                return f"{field} IS NULL"
            elif op == "$ne":
                return f"{field} IS NOT NULL" 
            val_str = "NULL"
        elif isinstance(val, (int, float)):
            val_str = str(val)
        elif isinstance(val, list):
            # handle lists for $in, $nin
            val_str = ", ".join(self._quote_if_needed(item) for item in val)
        else:
            # string or other
            val_str = f"'{self._escape_quotes(str(val))}'"

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
            # Handle NULL cases specially
            if val_str == "NULL":
                if sql_op == "=":
                    return f"{field} IS NULL"
                elif sql_op == "<>":
                    return f"{field} IS NOT NULL"
                
            # Handle regex/LIKE specially
            if op == "$regex":
                # Simple regex to SQL LIKE conversion
                # MongoDB: /pattern/ -> SQL: LIKE '%pattern%'
                # This is simplified and doesn't handle all regex cases
                pattern = str(val)
                if pattern.startswith('^'):
                    pattern = pattern[1:]
                    # Starts with
                    return f"{field} LIKE '{self._escape_quotes(pattern)}%'"
                elif pattern.endswith('$'):
                    pattern = pattern[:-1]
                    # Ends with
                    return f"{field} LIKE '%{self._escape_quotes(pattern)}'"
                else:
                    # Contains
                    return f"{field} LIKE '%{self._escape_quotes(pattern)}%'"
            
            # Regular comparison operators
            return f"{field} {sql_op} {val_str}"
        elif op == "$in":
            if not val or len(val) == 0:
                return f"FALSE"  # Empty $in is always false
            # e.g. field IN (1,2,3)
            return f"{field} IN ({val_str})"
        elif op == "$nin":
            if not val or len(val) == 0:
                return f"TRUE"  # Empty $nin is always true
            return f"{field} NOT IN ({val_str})"
        elif op == "$exists":
            # MongoDB $exists: true -> field IS NOT NULL
            if val:
                return f"{field} IS NOT NULL"
            else:
                return f"{field} IS NULL"
        else:
            # fallback
            return f"{field} /*unknown op {op}*/ {val_str}"

    def _build_order_by_sql(self, sort_list):
        """
        If we have "sort": [("age", 1), ("name", -1)],
        => "age ASC, name DESC"

        :param sort_list: List of tuples (field, direction) or dict
        :return: SQL ORDER BY clause as a string.
        """
        if not sort_list:
            return ""
            
        # Handle both list and dict formats
        if isinstance(sort_list, dict):
            sort_list = [(field, direction) for field, direction in sort_list.items()]
            
        if not isinstance(sort_list, list):
            return ""
            
        order_parts = []
        for field_dir in sort_list:
            if isinstance(field_dir, tuple) and len(field_dir) == 2:
                field, direction = field_dir
                dir_sql = "ASC" if direction == 1 else "DESC"
                order_parts.append(f"{field} {dir_sql}")
        return ", ".join(order_parts)

    def _quote_if_needed(self, val):
        """
        Return a numeric or quoted string

        :param val: The value to quote if it's a string.
        :return: The value as a string, quoted if it's a string.
        """
        if val is None:
            return "NULL"
        if isinstance(val, (int, float)):
            return str(val)
        return f"'{self._escape_quotes(str(val))}'"

    def _escape_quotes(self, s: str) -> str:
        """
        Simple approach to escape single quotes

        :param s: The string to escape.
        :return: The escaped string.
        """
        return s.replace("'", "''")

    def _handle_join_pipeline(self, mongo_obj: dict) -> str:
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
            where_sql = self._build_where_sql(match_stage)
            if where_sql:
                sql += f" WHERE {where_sql}"

        # ORDER BY from $sort
        sort_stage = next((stage.get('$sort') for stage in pipeline if '$sort' in stage), None)
        if sort_stage and isinstance(sort_stage, dict):
            sort_list = [(f, d) for f, d in sort_stage.items()]
            order_sql = self._build_order_by_sql(sort_list)
            if order_sql:
                sql += f" ORDER BY {order_sql}"

        # LIMIT stage
        limit_stage = next((stage.get('$limit') for stage in pipeline if '$limit' in stage), None)
        if isinstance(limit_stage, int):
            sql += f" LIMIT {limit_stage}"

        return sql + ";"


def mongo_query_to_sql(mongo_query: str) -> str:
    """
    Top-level function to convert MongoDB query (in shell syntax) to SQL.
    
    :param mongo_query: MongoDB query in shell syntax
    :return: SQL statement
    """
    converter = MongoToSql()
    return converter.convert(mongo_query)


# Example usage
if __name__ == "__main__":
    # READ examples
    read_examples = [
        'db.users.find({"age": {"$gt": 30}})',
        'db.users.find({"status": "active"}, {"name": 1, "email": 1}).sort({"name": 1}).limit(10)',
        'db.users.find({"$or": [{"age": {"$lt": 20}}, {"age": {"$gt": 60}}]})',
        'db.orders.aggregate([{"$match": {"status": "completed"}}, {"$group": {"_id": {"product": "$product", "region": "$region"}}}])'
    ]
    
    # CREATE examples
    create_examples = [
        'db.users.insertOne({"name": "John", "age": 30, "email": "john@example.com"})',
        'db.users.insertMany([{"name": "John", "age": 30}, {"name": "Jane", "age": 25}])'
    ]
    
    # UPDATE examples
    update_examples = [
        'db.users.updateOne({"_id": 1}, {"$set": {"status": "inactive"}})',
        'db.users.updateMany({"age": {"$gt": 50}}, {"$set": {"senior": true}})'
    ]
    
    # DELETE examples
    delete_examples = [
        'db.users.deleteOne({"_id": 1})',
        'db.users.deleteMany({"status": "inactive"})'
    ]
    
    # Print results
    print("READ OPERATIONS:")
    for example in read_examples:
        print(f"\nMongoDB: {example}")
        print(f"SQL: {mongo_query_to_sql(example)}")
    
    print("\nCREATE OPERATIONS:")
    for example in create_examples:
        print(f"\nMongoDB: {example}")
        print(f"SQL: {mongo_query_to_sql(example)}")
    
    print("\nUPDATE OPERATIONS:")
    for example in update_examples:
        print(f"\nMongoDB: {example}")
        print(f"SQL: {mongo_query_to_sql(example)}")