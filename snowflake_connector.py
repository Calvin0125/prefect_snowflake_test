from prefect_snowflake import SnowflakeCredentials, SnowflakeConnector
from snowflake_credentials import credentials

creds = credentials.load("my-new-snowflake-credentials")

connector = SnowflakeConnector(
    credentials=creds,
    database="PREFECT_TEST",
    schema="PUBLIC",
    warehouse="COMPUTE_WH",
)
connector.save("my-new-snowflake-connector", overwrite=True)