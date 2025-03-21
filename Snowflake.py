import streamlit as st
import pandas as pd
import time
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any

# =================== MIGRATION FUNCTIONS ===================

def test_connection(conn_params: Dict[str, Any]) -> bool:
    """
    Test if connection parameters are valid.
    
    Args:
        conn_params: Connection parameters
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False

def get_tables(conn_params: Dict[str, Any]) -> List[str]:
    """
    Get all tables from the specified schema.
    
    Args:
        conn_params: Connection parameters
        
    Returns:
        List of table names
    """
    tables = []
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
 
        # Fetch all table names from source schema
        cursor.execute(f"SELECT TABLE_NAME FROM {conn_params['database']}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{conn_params['schema']}'")
        tables = [row[0] for row in cursor.fetchall()]
 
    except Exception as e:
        raise Exception(f"Error fetching tables: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
    return tables

def migrate_table(table: str, source_params: Dict[str, Any], target_params: Dict[str, Any], batch_size: int = 2000) -> Dict[str, Any]:
    """
    Migrate a single table from source to target Snowflake instance.
    
    Args:
        table: Table name to migrate
        source_params: Source connection parameters
        target_params: Target connection parameters
        batch_size: Number of rows to process in each batch
        
    Returns:
        Dict with migration status and details
    """
    start_time = time.time()
    result = {
        "table": table,
        "success": False,
        "rows_migrated": 0,
        "error": None,
        "time_taken": 0
    }
    
    try:
        source_conn = snowflake.connector.connect(**source_params)
        source_cursor = source_conn.cursor()
 
        target_conn = snowflake.connector.connect(**target_params)
        target_cursor = target_conn.cursor()
 
        # Fetch CREATE TABLE statement
        source_cursor.execute(f"SELECT GET_DDL('TABLE', '{source_params['database']}.{source_params['schema']}.{table}')")
        create_table_statement = source_cursor.fetchone()[0]
 
        # Replace schema/database references for target
        create_table_statement = create_table_statement.replace(
            f"{source_params['database']}.{source_params['schema']}",
            f"{target_params['database']}.{target_params['schema']}"
        )
 
        # Create the table in the target schema
        target_cursor.execute(f"DROP TABLE IF EXISTS {target_params['database']}.{target_params['schema']}.{table}")
        target_cursor.execute(create_table_statement)
        target_conn.commit()
 
        # Fetch column names
        source_cursor.execute(f"SELECT COLUMN_NAME FROM {source_params['database']}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{source_params['schema']}'")
        columns = [row[0] for row in source_cursor.fetchall()]
        column_list = ', '.join(columns)
 
        # Copy data in batches
        offset = 0
        total_rows = 0
        while True:
            source_cursor.execute(f"""
                SELECT {column_list} FROM {source_params['database']}.{source_params['schema']}.{table}
                LIMIT {batch_size} OFFSET {offset}
            """)
            rows = source_cursor.fetchall()
            
            if not rows:
                break
 
            insert_query = f"""
                INSERT INTO {target_params['database']}.{target_params['schema']}.{table} ({column_list})
                VALUES ({', '.join(['%s'] * len(columns))})
            """
            target_cursor.executemany(insert_query, rows)
            target_conn.commit()
            
            offset += batch_size
            total_rows += len(rows)
            result["rows_migrated"] = total_rows
 
        result["success"] = True

    except Exception as e:
        result["error"] = str(e)
    finally:
        if 'source_cursor' in locals():
            source_cursor.close()
        if 'source_conn' in locals():
            source_conn.close()
        if 'target_cursor' in locals():
            target_cursor.close()
        if 'target_conn' in locals():
            target_conn.close()
            
        result["time_taken"] = round(time.time() - start_time, 2)
        return result

def migrate_all_tables(source_params: Dict[str, Any], target_params: Dict[str, Any], batch_size: int = 2000, max_workers: int = 4) -> List[Dict[str, Any]]:
    """
    Migrate all tables from source to target Snowflake instance.
    
    Args:
        source_params: Source connection parameters
        target_params: Target connection parameters
        batch_size: Number of rows to process in each batch
        max_workers: Number of concurrent migrations
        
    Returns:
        List of migration results
    """
    # Fetch all table names from source schema
    tables = get_tables(source_params)
    results = []
    
    # Use ThreadPoolExecutor to migrate tables concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {
            executor.submit(migrate_table, table, source_params, target_params, batch_size): table 
            for table in tables
        }
        
        for future in future_to_table:
            results.append(future.result())
    
    return results

# =================== STREAMLIT APP ===================

st.set_page_config(
    page_title="Snowflake Migration Tool",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .success-message {
        color: #0f5132;
        background-color: #d1e7dd;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #badbcc;
    }
    .error-message {
        color: #842029;
        background-color: #f8d7da;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #f5c2c7;
    }
    .info-message {
        color: #055160;
        background-color: #cff4fc;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #b6effb;
    }
</style>
""", unsafe_allow_html=True)

st.title("❄️ Snowflake Data Migration Tool")
st.write("Easily migrate tables between Snowflake databases and schemas")

# Initialize session state variables
if 'migration_results' not in st.session_state:
    st.session_state.migration_results = None
if 'source_tables' not in st.session_state:
    st.session_state.source_tables = []
if 'migration_in_progress' not in st.session_state:
    st.session_state.migration_in_progress = False

# Sidebar for connection parameters
with st.sidebar:
    st.header("Connection Settings")
    
    # Source connection parameters
    st.subheader("Source Connection")
    source_account = st.text_input("Account", key="source_account", help="Format: orgname-accountname")
    source_user = st.text_input("Username", key="source_user")
    source_password = st.text_input("Password", key="source_password", type="password")
    source_warehouse = st.text_input("Warehouse", key="source_warehouse", value="COMPUTE_WH")
    source_database = st.text_input("Database", key="source_database")
    source_schema = st.text_input("Schema", key="source_schema")
    
    # Target connection parameters
    st.subheader("Target Connection")
    target_account = st.text_input("Account", key="target_account", help="Format: orgname-accountname")
    target_user = st.text_input("Username", key="target_user")
    target_password = st.text_input("Password", key="target_password", type="password")
    target_warehouse = st.text_input("Warehouse", key="target_warehouse", value="COMPUTE_WH")
    target_database = st.text_input("Database", key="target_database")
    target_schema = st.text_input("Schema", key="target_schema")
    
    # Migration settings
    st.subheader("Migration Settings")
    batch_size = st.number_input("Batch Size", min_value=100, max_value=10000, value=2000, step=100,
                                help="Number of rows to process in each batch")
    max_workers = st.number_input("Concurrent Tables", min_value=1, max_value=8, value=4, step=1,
                                help="Number of tables to migrate simultaneously")
    
    # Test connection buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Test Source"):
            source_params = {
                "user": source_user,
                "password": source_password,
                "account": source_account,
                "warehouse": source_warehouse,
                "database": source_database,
                "schema": source_schema
            }
            
            with st.spinner("Testing source connection..."):
                if test_connection(source_params):
                    st.success("Connection successful!")
                    try:
                        st.session_state.source_tables = get_tables(source_params)
                        st.info(f"Found {len(st.session_state.source_tables)} tables")
                    except Exception as e:
                        st.error(f"Error listing tables: {e}")
                else:
                    st.error("Connection failed")
    
    with col2:
        if st.button("Test Target"):
            target_params = {
                "user": target_user,
                "password": target_password,
                "account": target_account,
                "warehouse": target_warehouse,
                "database": target_database,
                "schema": target_schema
            }
            
            with st.spinner("Testing target connection..."):
                if test_connection(target_params):
                    st.success("Connection successful!")
                else:
                    st.error("Connection failed")

# Main content
tab1, tab2 = st.tabs(["Migration", "Results"])

with tab1:
    # Table selection
    if st.session_state.source_tables:
        st.header("Select Tables to Migrate")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_tables = st.multiselect(
                "Choose tables", 
                options=st.session_state.source_tables,
                default=st.session_state.source_tables,
                help="Select the tables you want to migrate"
            )
        
        with col2:
            select_all = st.checkbox("Select All", value=True)
            if select_all:
                selected_tables = st.session_state.source_tables
        
        # Start migration button
        if st.button("Start Migration", type="primary", disabled=st.session_state.migration_in_progress):
            if not selected_tables:
                st.warning("Please select at least one table to migrate")
            else:
                source_params = {
                    "user": source_user,
                    "password": source_password,
                    "account": source_account,
                    "warehouse": source_warehouse,
                    "database": source_database,
                    "schema": source_schema
                }
                
                target_params = {
                    "user": target_user,
                    "password": target_password,
                    "account": target_account,
                    "warehouse": target_warehouse,
                    "database": target_database,
                    "schema": target_schema
                }
                
                st.session_state.migration_in_progress = True
                
                progress_bar = st.progress(0.0)
                status_text = st.empty()
                
                # Create placeholders for each table's status
                table_statuses = {table: st.empty() for table in selected_tables}
                
                # Start migration for each selected table individually
                results = []
                for i, table in enumerate(selected_tables):
                    status_text.text(f"Migrating table {i+1}/{len(selected_tables)}: {table}")
                    table_statuses[table].markdown(f"⏳ Migrating `{table}`...")
                    
                    result = migrate_table(table, source_params, target_params, batch_size)
                    results.append(result)
                    
                    if result["success"]:
                        table_statuses[table].markdown(f"✅ `{table}`: Migrated {result['rows_migrated']} rows in {result['time_taken']} seconds")
                    else:
                        table_statuses[table].markdown(f"❌ `{table}`: Failed - {result['error']}")
                    
                    # Update progress bar
                    progress_bar.progress((i + 1) / len(selected_tables))
                
                st.session_state.migration_results = results
                st.session_state.migration_in_progress = False
                status_text.text("Migration completed!")
                
                # Add summary
                success_count = sum(1 for r in results if r["success"])
                total_rows = sum(r["rows_migrated"] for r in results if r["success"])
                
                summary = f"""
                <div class="success-message">
                <b>Migration Summary:</b><br>
                ✅ Successfully migrated: {success_count}/{len(results)} tables<br>
                ❌ Failed: {len(results) - success_count} tables<br>
                📊 Total rows migrated: {total_rows}<br>
                ⏱️ Total time: {sum(r["time_taken"] for r in results):.2f} seconds
                </div>
                """
                st.markdown(summary, unsafe_allow_html=True)
    
    else:
        st.info("Connect to source database to view available tables")

with tab2:
    if st.session_state.migration_results:
        st.header("Migration Results")
        
        # Create a DataFrame from results
        df = pd.DataFrame(st.session_state.migration_results)
        df = df[["table", "success", "rows_migrated", "time_taken", "error"]]
        df.columns = ["Table", "Success", "Rows Migrated", "Time (sec)", "Error"]
        
        # Success/Failure metrics
        col1, col2, col3 = st.columns(3)
        success_count = sum(1 for r in st.session_state.migration_results if r["success"])
        
        with col1:
            st.metric("Total Tables", len(st.session_state.migration_results))
        with col2:
            st.metric("Successful", success_count)
        with col3:
            st.metric("Failed", len(st.session_state.migration_results) - success_count)
        
        # Show results as a table
        st.dataframe(df, use_container_width=True)
        
        # Show any errors
        errors = [(r["table"], r["error"]) for r in st.session_state.migration_results if not r["success"]]
        if errors:
            st.subheader("Error Details")
            for table, error in errors:
                st.error(f"Error in table '{table}': {error}")
    else:
        st.info("Run a migration to see results here")

# Footer
st.markdown("---")
st.markdown("Created with Streamlit • [View on GitHub](https://github.com/Vamsi5739/MIGRATION.git)")

# Run the app
if __name__ == "__main__":
    # This is automatically handled by Streamlit
    pass
