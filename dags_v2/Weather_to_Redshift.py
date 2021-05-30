import json
import requests
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def Weather_to_Redshift():
    """
    ### TaskFlow API Tutorial Documentation
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    def get_Redshift_connection():
        # autocommit is False by default
        hook = PostgresHook(postgres_conn_id='redshift_dev_db')
        return hook.get_conn().cursor()

    @task()
    def extract():
        lat = 37.5665
        lon = 126.9780
        api_key = Variable.get("open_weather_api_key")

        # https://openweathermap.org/api/one-call-api
        url = "https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&appid=%s&units=metric" % (lat, lon, api_key)
        response = requests.get(url)
        data = json.loads(response.text)

        return data

    @task()
    def transform(data: dict):
        ret = []
        for d in data["daily"]:
            day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
            ret.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
        return ret

    @task()
    def load(ret):
        print(ret)
        cur = get_Redshift_connection()
        insert_sql = """DELETE FROM keeyong.weather_forecast;INSERT INTO keeyong.weather_forecast VALUES """ + ",".join(ret)
        logging.info(insert_sql)
        try:
            cur.execute(insert_sql)
            cur.execute("Commit;")
        except Exception as e:
            cur.execute("Rollback;")

    """
    Main
    """
    data = extract()
    ret = transform(data)
    load(ret)


W2R = Weather_to_Redshift()
