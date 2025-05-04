"""
Examples of using the SQL to MongoDB converter.
These examples demonstrate how to convert SQL queries to MongoDB commands.
"""

from sql_to_mongo import sql_to_mongo

def basic_examples():
    """
    Basic examples showing all CRUD operations.
    """
    print("=== SELECT Examples ===")
    # Simple SELECT
    sql = "SELECT * FROM users;"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    # SELECT with WHERE
    sql = "SELECT name, age FROM users WHERE age > 30;"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    # SELECT with ORDER BY and LIMIT
    sql = "SELECT name, email FROM users ORDER BY name ASC LIMIT 10;"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    print("=== INSERT Examples ===")
    # Simple INSERT
    sql = "INSERT INTO users (name, age, email) VALUES ('John', 25, 'john@example.com');"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    # Multiple INSERT
    sql = "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 35), ('Charlie', 40);"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    print("=== UPDATE Examples ===")
    # Simple UPDATE
    sql = "UPDATE users SET age = 26 WHERE name = 'John';"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    # UPDATE multiple fields
    sql = "UPDATE users SET age = 31, status = 'active' WHERE email = 'alice@example.com';"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    print("=== DELETE Examples ===")
    # Simple DELETE
    sql = "DELETE FROM users WHERE name = 'John';"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    # DELETE all
    sql = "DELETE FROM inactive_users;"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()

def advanced_examples():
    """
    Advanced examples showing more complex queries.
    """
    print("=== Advanced SELECT Examples ===")
    # GROUP BY example
    sql = "SELECT department, COUNT(*) FROM employees GROUP BY department;"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    # JOIN example
    sql = "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100;"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()
    
    # Complex WHERE conditions
    sql = "SELECT * FROM products WHERE price < 50 AND category = 'electronics' AND stock > 0;"
    print(f"SQL: {sql}")
    print(f"MongoDB: {sql_to_mongo(sql)}")
    print()

if __name__ == "__main__":
    basic_examples()
    advanced_examples()

# Expected output would show converted MongoDB commands like:
# SQL: SELECT * FROM users;
# MongoDB: db.users.find({})
#
# SQL: SELECT name, age FROM users WHERE age > 30;
# MongoDB: db.users.find({ age: { $gt: 30 } }, { name: 1, age: 1 })
#
# SQL: INSERT INTO users (name, age, email) VALUES ('John', 25, 'john@example.com');
# MongoDB: db.users.insertOne({ name: "John", age: 25, email: "john@example.com" })
#
# SQL: UPDATE users SET age = 26 WHERE name = 'John';
# MongoDB: db.users.updateMany({ name: "John" }, { $set: { age: 26 } })
#
# SQL: DELETE FROM users WHERE name = 'John';
# MongoDB: db.users.deleteMany({ name: "John" })