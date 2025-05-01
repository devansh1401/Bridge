# Query Translator: Effortless SQL ↔ MongoDB Query Conversion

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat&logo=opensource)](LICENSE)
[![Python Version](https://img.shields.io/badge/Python-%3E=3.7-brightgreen.svg?style=flat&logo=python)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-%23E34F26.svg?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-%23471240.svg?style=flat&logo=mongodb)](https://www.mongodb.com/)

**Query Translator** is a Python toolkit designed to seamlessly convert SQL queries into MongoDB query dictionaries and vice versa. Whether you're prototyping, migrating data models, or just want to bridge the gap between SQL and NoSQL, this library makes the process straightforward and fast—no heavy ORM required.

---

## Overview

- **Bidirectional Conversion:** Effortlessly translate SQL SELECT statements to MongoDB queries and convert MongoDB find dictionaries back to SQL.
- **Flexible & Extensible:** Handles a variety of query patterns, including complex WHERE clauses, logical operators, and projections. Easily adaptable for more advanced use cases.
- **Developer-Friendly:** Minimal dependencies, clear API, and ready-to-run examples and tests.

---

## Getting Started

### Requirements

- Python 3.7+
- pip

### Installation

Clone this repository and install the required dependencies:

```bash
git clone https://github.com/yourusername/query-translator.git
cd query-translator
pip install -r requirements.txt
python setup.py install
```

#### For Testing/Development

To run tests or contribute, you may want to install `pytest`:

```bash
pip install pytest
```

---

## How to Use

### SQL to MongoDB Example

Convert a SQL SELECT query into a MongoDB-style dictionary:

```python
from query_translator import sql_to_mongo

sql_query = "SELECT name, age FROM users WHERE age > 30 AND name = 'Alice';"
mongo_query = sql_to_mongo(sql_query)
print(mongo_query)
# Output:
# {
#   "collection": "users",
#   "find": {"age": {"$gt": 30}, "name": "Alice"},
#   "projection": {"name": 1, "age": 1}
# }
```

### MongoDB to SQL Example

Convert a MongoDB query dictionary into a SQL SELECT statement:

```python
from query_translator import mongo_to_sql

mongo_obj = {
    "collection": "users",
    "find": {
        "$or": [
            {"age": {"$gte": 25}},
            {"status": "ACTIVE"}
        ],
        "tags": {"$in": ["dev", "qa"]}
    },
    "projection": {"age": 1, "status": 1, "tags": 1},
    "sort": [("age", 1), ("name", -1)],
    "limit": 10,
    "skip": 5
}
sql_query = mongo_to_sql(mongo_obj)
print(sql_query)
# Output:
# SELECT age, status, tags FROM users WHERE ((age >= 25) OR (status = 'ACTIVE')) AND (tags IN ('dev', 'qa'))
# ORDER BY age ASC, name DESC LIMIT 10 OFFSET 5;
```

---

## API Summary

- `sql_to_mongo(sql_query: str) -> dict`  
  Parses a SQL SELECT query and returns a MongoDB query dictionary with `collection`, `find`, and `projection` keys.

- `mongo_to_sql(mongo_obj: dict) -> str`  
  Converts a MongoDB-style query dictionary into a SQL SELECT statement string.

---

## Running Tests

A suite of unit tests is included to verify the conversion logic. To run all tests:

```bash
python -m unittest discover tests
# or, if you prefer pytest:
pytest --maxfail=1 --disable-warnings -q
```

You can also try out the demo script:

```bash
python tests/demo.py
```

---

## Contributing

Contributions are welcome! If you have ideas for improvements or new features, feel free to fork the repo and submit a pull request. For major changes, please open an issue to discuss your proposal first.

---

## License

Distributed under the [MIT License](LICENSE).

---

## Why Query Translator?

This project is ideal for developers who need to quickly move between SQL and MongoDB data models, or who want to experiment with query translation without the overhead of a full ORM or database migration tool. Extend and adapt it to fit your needs!

Happy coding!
