from prefect import task, flow
from prefect_snowflake import SnowflakeConnector
from prefect.deployments import Deployment
from snowflake_connector import connector
import boto3
import botocore
import json

@task
def setup_table(block_name: str) -> None:
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket='covid19-lake', Key='rearc-covid-19-testing-data/json/states_daily/part-00000-c2ee520b-8fff-4174-b5d8-427ca1ea1015-c000.json')
        data = response['Body'].read()
        string = data.decode('utf-8')
        lines = string.split('\n')
        states = {}
        for line in lines:
            if line:
                covid_json = json.loads(line)
                state = covid_json['state']
                if state in states:
                    states[state]['positive'] += float(covid_json.get('positive', 0))
                    states[state]['hospitalized'] += float(covid_json.get('hospitalized', 0))
                else:
                    states[state] = {'positive': float(covid_json.get('positive', 0)), 'hospitalized': float(covid_json.get('hospitalized', 0))}

        return states
    except botocore.exceptions.ClientError as e:
        print(f"Error downloading file: {e}")
        return False

@task
def populate_table(data, block_name: str) -> None:
    rows = []
    for state in data:
        row = {}
        row['state'] = state
        if data[state]['positive'] == 0:
            hospitalized_percentage = 0
        else:
            hospitalized_percentage = data[state]['hospitalized'] / data[state]['positive']
        row['hospitalized'] = hospitalized_percentage
        rows.append(row)

    with SnowflakeConnector.load(block_name) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS states_hospitalized_percentage (state string, hospitalized float);")
        conn.execute_many("INSERT INTO states_hospitalized_percentage (state, hospitalized) VALUES (%(state)s, %(hospitalized)s);", seq_of_parameters=rows)

@flow
def snowflake_flow(block_name: str):
    data = setup_table(block_name)
    populate_table(data, block_name)

def deploy():
    deployment = Deployment.build_from_flow(flow=snowflake_flow, name="snowflake-prefect-test")
    deployment.apply()

deploy()
#snowflake_flow('my-new-snowflake-connector')