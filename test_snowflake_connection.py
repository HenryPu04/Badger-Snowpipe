import snowflake.connector

# Snowflake connection details
SNOWFLAKE_ACCOUNT = 'zp03831.us-east-2.aws'
SNOWFLAKE_USER = 'HYP2'
SNOWFLAKE_PASSWORD = 'Relove1234!'
SNOWFLAKE_DATABASE = 'BADGER'
SNOWFLAKE_SCHEMA = 'PUBLIC'

try:
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    print("Connection to Snowflake successful!")
    conn.close()
except Exception as e:
    print("Failed to connect to Snowflake")
    print(e)
