from prefect_snowflake import SnowflakeCredentials

credentials = SnowflakeCredentials(
    account="JOASXWY-CS65710",
    user="Calvin0125",
    password="1$WurgM*uVd!97Rl"
)
credentials.save("my-new-snowflake-credentials", overwrite=True)