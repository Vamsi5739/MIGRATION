# Snowflake Migration Tool

A Streamlit web application that helps you migrate tables between Snowflake databases or instances.

## Features

- Migrate tables between different Snowflake accounts
- Connect to source and target Snowflake instances
- Select specific tables to migrate
- Concurrent table migration for faster processing
- Configurable batch size for large tables
- Progress tracking and detailed results

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/snowflake-migration-tool.git
   cd snowflake-migration-tool
   ```

2. Create a virtual environment and install dependencies:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use: venv\Scripts\activate
   pip install -r requirements.txt
   ```

## Usage

1. Start the Streamlit app:
   ```
   streamlit run app.py
   ```

2. Open your browser and navigate to http://localhost:8501

3. Enter your Snowflake connection details for both source and target

4. Test your connections

5. Select tables to migrate

6. Start the migration

## Configuration

You can adjust the following settings in the app:

- **Batch Size**: Number of rows to fetch and insert in each batch (default: 2000)
- **Concurrent Tables**: Number of tables to migrate simultaneously (default: 4)

## Security

- Passwords are masked in the UI
- Connection details are not stored between sessions
- For production use, consider using Streamlit secrets or environment variables

## License

MIT
