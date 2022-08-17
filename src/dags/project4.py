import logging

from datetime import datetime, timedelta
import pendulum

from airflow.decorators import dag, task

from airflow.hooks.base import BaseHook

from airflow.models import Variable

import pandas as pd
import numpy

import requests
import psycopg2
from psycopg2.extensions import register_adapter, AsIs

import json

from collections import namedtuple


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

pg_conn_wh = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')

log = logging.getLogger(__name__)

sql_path = '/lessons/dags/sql'


# функция под запись в постгрес
def pg_execute_query_void(query, conn_obj):
    conn_args = {
        'dbname': conn_obj.schema,
        'user': conn_obj.login,
        'password': conn_obj.password,
        'host': conn_obj.host,
        'port': conn_obj.port
    }
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

# забрать ответ по api, отдать json
def get_api_response(conn_name, api_url, api_headers):
    conn = BaseHook.get_connection(conn_name)
    req_host = conn.host
    nickname = "sergei_baranov"
    cohort = "1"
    resp = requests.get(
        f"{req_host}{api_url}",
        headers=api_headers
    ).json()
    return resp


# однотипная работа перелить api-операцию в stg-приёмник
# пролистыванием до упора с апсертом по object_id
def fill_stg(api_operation_name, table_name, id_field):
    continue_marker = True # do-while
    next_offset = 0
    limit = 50
    inf_counter = 0
    while continue_marker is True and inf_counter < 100:
        # sort_field=id&
        api_url = "/{}?sort_direction=asc&limit={}&offset={}".format(
            api_operation_name, limit, next_offset)
        api_headers={
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
            "X-Nickname": "sergei_baranov",
            "X-Cohort": "1"
        }
        json_resp = get_api_response('http_conn_id', api_url, api_headers)

        if len(json_resp) > 0:
            continue_marker = True
            next_offset += limit
            inf_counter += 1
        else:
            continue_marker = False
            break

        for rec in json_resp:
            object_id = rec[id_field]
            object_value = json.dumps(rec)
            update_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            upsert_qr = """
                INSERT INTO {tbl}
                    (object_id, object_value, update_ts)
                VALUES (
                    '{ins_object_id}',
                    '{ins_object_value}',
                    '{ins_update_ts}'::timestamp(0)
                )
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts
            """.format(
                tbl=table_name,
                ins_object_id=object_id,
                ins_object_value=object_value,
                ins_update_ts=update_ts
            )
            pg_execute_query_void(upsert_qr, pg_conn_wh)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 8, 3, tz="UTC"),
    catchup=False,
    tags=['project4', 'project4init', 'project4stg'],
    is_paused_upon_creation=False
)
def project4_dag():

    @task()
    def init_task():
        with open(sql_path + '/init.sql') as file:
            init_query = file.read()
        pg_execute_query_void(init_query, pg_conn_wh)


    @task()
    def restaurants_task():
        api_operation_name = 'restaurants'
        table_name = 'stg.deliverysystem_restaurants'
        id_field = '_id'
        fill_stg(api_operation_name, table_name, id_field)


    @task()
    def couriers_task():
        api_operation_name = 'couriers'
        table_name = 'stg.deliverysystem_couriers'
        id_field = '_id'
        fill_stg(api_operation_name, table_name, id_field)


    @task()
    def deliveries_task():
        api_operation_name = 'deliveries'
        table_name = 'stg.deliverysystem_deliveries'
        id_field = 'delivery_id'
        fill_stg(api_operation_name, table_name, id_field)


    init = init_task()
    restaurants = restaurants_task()
    couriers = couriers_task()
    deliveries = deliveries_task()

    init >> [restaurants, couriers, deliveries] # type: ignore

project_dag = project4_dag()