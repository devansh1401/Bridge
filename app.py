import streamlit as st
import json
from query_translator import sql_to_mongo, mongo_to_sql

st.set_page_config(page_title="Query Translator", page_icon="🔄", layout="centered")
st.title("🔄 Query Translator")
st.caption("Effortlessly convert between SQL and MongoDB queries.")

# Tabs for conversion directions
tab1, tab2 = st.tabs(["SQL → MongoDB", "MongoDB → SQL"])

with tab1:
    st.subheader("SQL to MongoDB")
    sql_type = st.selectbox(
        "SQL operation type:",
        ["SELECT", "INSERT", "UPDATE", "DELETE"],
        key="sql_type"
    )
    
    placeholders = {
        "SELECT": "e.g. SELECT name, age FROM users WHERE age > 30 AND name = 'Alice';",
        "INSERT": "e.g. INSERT INTO users (name, age, email) VALUES ('John', 30, 'john@example.com');",
        "UPDATE": "e.g. UPDATE users SET age = 31 WHERE name = 'John';",
        "DELETE": "e.g. DELETE FROM users WHERE status = 'inactive';"
    }
    
    sql_input = st.text_area(
        "Enter your SQL query:",
        height=150,
        placeholder=placeholders[sql_type]
    )
    
    if st.button("Convert to MongoDB", key="sql2mongo"):
        if not sql_input.strip():
            st.warning(f"Please enter a SQL {sql_type} query.")
        else:
            try:
                mongo_result = sql_to_mongo(sql_input)
                st.success("MongoDB Command:")
                st.code(mongo_result, language="javascript")
            except Exception as e:
                st.error(f"Error: {e}")

with tab2:
    st.subheader("MongoDB to SQL")
    mongo_input = st.text_area(
        "Enter your MongoDB query:",
        height=200,
        placeholder='e.g. db.users.find({"age": {"$gte": 25}}, {"age": 1})'
    )
    if st.button("Convert to SQL", key="mongo2sql"):
        if not mongo_input.strip():
            st.warning("Please enter a MongoDB query.")
        else:
            try:
                sql_result = mongo_to_sql(mongo_input)
                st.success("SQL Query:")
                st.code(sql_result, language="sql")
            except Exception as e:
                st.error(f"Error: {e}")

st.markdown("---")
st.caption("Made with ❤️ using Streamlit and Query Translator.") 