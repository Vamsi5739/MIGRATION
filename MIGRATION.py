import streamlit as st
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor

def migrate_table(table, source_params, target_params, batch_size, log_container):
    try:
        source_conn = snowflake.connector.connect(**source_params)
        source_cursor = source_conn.cursor()
        target_conn = snowflake.connector.connect(**target_params)
        target_cursor = target_conn.cursor()

        log_container.write(f"🔄 Migrating table: {table}")

        # Fetch CREATE TABLE statement
        source_cursor.execute(f"SELECT GET_DDL('TABLE', '{source_params['database']}.{source_params['schema']}.{table}')")
        create_table_statement = source_cursor.fetchone()[0]
        create_table_statement = create_table_statement.replace(
            f"{source_params['database']}.{source_params['schema']}",
            f"{target_params['database']}.{target_params['schema']}"
        )
        
        # Create the table in the target schema
        target_cursor.execute(f"DROP TABLE IF EXISTS {target_params['database']}.{target_params['schema']}.{table}")
        target_cursor.execute(create_table_statement)
        target_conn.commit()
        log_container.write(f"✅ Table {table} created in target schema.")
        
        # Fetch column names
        source_cursor.execute(f"SELECT COLUMN_NAME FROM {source_params['database']}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{source_params['schema']}'")
        columns = [row[0] for row in source_cursor.fetchall()]
        column_list = ', '.join(columns)
        
        # Copy data in batches
        offset = 0
        while True:
            source_cursor.execute(f"SELECT {column_list} FROM {source_params['database']}.{source_params['schema']}.{table} LIMIT {batch_size} OFFSET {offset}")
            rows = source_cursor.fetchall()
            if not rows:
                break
            insert_query = f"INSERT INTO {target_params['database']}.{target_params['schema']}.{table} ({column_list}) VALUES ({', '.join(['%s'] * len(columns))})"
            target_cursor.executemany(insert_query, rows)
            target_conn.commit()
            offset += batch_size
            log_container.write(f"✅ {batch_size} rows inserted for table: {table}")
        
        log_container.write(f"✅ Data migration completed for table: {table}")
    
    except Exception as e:
        log_container.write(f"❌ Error migrating table {table}: {e}")
    finally:
        source_cursor.close()
        source_conn.close()
        target_cursor.close()
        target_conn.close()

def migrate_all_tables(source_params, target_params, batch_size, log_container):
    try:
        source_conn = snowflake.connector.connect(**source_params)
        source_cursor = source_conn.cursor()
        source_cursor.execute(f"SELECT TABLE_NAME FROM {source_params['database']}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{source_params['schema']}'")
        tables = [row[0] for row in source_cursor.fetchall()]
        log_container.write(f"✅ Found {len(tables)} tables: {tables}")
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            for table in tables:
                executor.submit(migrate_table, table, source_params, target_params, batch_size, log_container)
    
    except Exception as e:
        log_container.write(f"❌ Error fetching tables: {e}")
    finally:
        source_cursor.close()
        source_conn.close()

# Streamlit UI
st.title("Snowflake Migration Tool")
with st.form("snowflake_form"):
    st.subheader("Source Snowflake Credentials")
    source_user = st.text_input("User", "", type="default")
    source_password = st.text_input("Password", "", type="password")
    source_account = st.text_input("Account", "")
    source_warehouse = st.text_input("Warehouse", "")
    source_database = st.text_input("Database", "")
    source_schema = st.text_input("Schema", "")
    
    st.subheader("Target Snowflake Credentials")
    target_user = st.text_input("User", "", type="default")
    target_password = st.text_input("Password", "", type="password")
    target_account = st.text_input("Account", "")
    target_warehouse = st.text_input("Warehouse", "")
    target_database = st.text_input("Database", "")
    target_schema = st.text_input("Schema", "")
    
    batch_size = st.number_input("Batch Size", min_value=100, max_value=10000, value=2000, step=100)
    submit_button = st.form_submit_button("Start Migration")

log_container = st.empty()

if submit_button:
    source_params = {
        "user": source_user,
        "password": source_password,
        "account": source_account,
        "warehouse": source_warehouse,
        "database": source_database,
        "schema": source_schema,
    }
    target_params = {
        "user": target_user,
        "password": target_password,
        "account": target_account,
        "warehouse": target_warehouse,
        "database": target_database,
        "schema": target_schema,
    }
    
    migrate_all_tables(source_params, target_params, batch_size, log_container)
