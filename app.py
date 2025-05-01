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
    sql_input = st.text_area(
        "Enter your SQL SELECT query:",
        height=150,
        placeholder="e.g. SELECT name, age FROM users WHERE age > 30 AND name = 'Alice';"
    )
    if st.button("Convert to MongoDB", key="sql2mongo"):
        if not sql_input.strip():
            st.warning("Please enter a SQL query.")
        else:
            try:
                mongo_result = sql_to_mongo(sql_input)
                st.success("MongoDB Query:")
                st.code(json.dumps(mongo_result, indent=2), language="json")
            except Exception as e:
                st.error(f"Error: {e}")

with tab2:
    st.subheader("MongoDB to SQL")
    mongo_input = st.text_area(
        "Enter your MongoDB query as JSON:",
        height=200,
        placeholder='e.g. {\n  "collection": "users",\n  "find": {"age": {"$gte": 25}},\n  "projection": {"age": 1}\n}'
    )
    if st.button("Convert to SQL", key="mongo2sql"):
        if not mongo_input.strip():
            st.warning("Please enter a MongoDB query in JSON format.")
        else:
            try:
                mongo_obj = json.loads(mongo_input)
                sql_result = mongo_to_sql(mongo_obj)
                st.success("SQL Query:")
                st.code(sql_result, language="sql")
            except json.JSONDecodeError:
                st.error("Invalid JSON. Please check your MongoDB query input.")
            except Exception as e:
                st.error(f"Error: {e}")

st.markdown("---")
st.caption("Made with ❤️ using Streamlit and Query Translator.") 