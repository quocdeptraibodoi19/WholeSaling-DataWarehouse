import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from fastapi import APIRouter, Response, status, Path, Query, Depends, Body

import pandas as pd

from typing import Annotated

from datetime import datetime

import traceback

import json

from router.authentication import get_current_user
from core.connections import DataWarehouseConnection, OperationalDBConnection
from core.data_processor import DataDeserializor, DataDeserializationLevel
from core.color_manager import ColorManager
from core.chart_generator import ChartGeneratorManager
from core.query_parser import SelectedFact, SelectedDim, QueryParserManager
from models.chart_metadata import ClientChartMetaData
from models.chart import (
    BarChart,
    PieChart,
    LineChart,
    MapChart,
    ChartMetaData,
    FetchedChartMetaData,
    FetchedDataWidget,
    DashBoard,
)
from models.user import User
from src.constants import ConstantProvider

router = APIRouter(prefix="/charts", tags=["charts"])


@router.post("/preview", response_model=ChartMetaData)
def preview_chart(
    client_chart_metadata: ClientChartMetaData,
    catched_colors: Annotated[list[str], Body()] = None,
):
    data_warehouse_connection = DataWarehouseConnection()
    try:
        data_warehouse_connection.connect()

        client_dimensions_metadata = client_chart_metadata.dimensions

        dimensions = []
        global_dim_columns = []
        is_dim_date_quarter = False
        for dimension in client_dimensions_metadata:
            if (
                dimension.dim_name == ConstantProvider.dim_date_name()
                and dimension.dim_column == ConstantProvider.dim_date_quarter_column()
            ):
                is_dim_date_quarter = True
                dim_columns = [
                    ConstantProvider.dim_date_year_column(),
                    ConstantProvider.dim_date_quarter_column(),
                ]
            else:
                dim_columns = [dimension.dim_column]
                global_dim_columns += dim_columns

            dim_name = dimension.dim_name
            dim_key = dimension.dim_key
            ref_fact_key = dimension.ref_fact_key
            dim_condition = dimension.dim_condition

            dimensions.append(
                SelectedDim(dim_name, dim_columns, dim_key, ref_fact_key, dim_condition)
            )

        if is_dim_date_quarter:
            global_dim_columns = [
                ConstantProvider.dim_date_year_column(),
                ConstantProvider.dim_date_quarter_column(),
            ] + global_dim_columns

        selected_fact = SelectedFact(
            client_chart_metadata.fact_name,
            client_chart_metadata.fact_column,
            dimensions,
        )

        query_parser_manager = QueryParserManager()
        query = query_parser_manager.parse_query(selected_fact)
        print(f"The generated query is: {query}")

        chart_data = pd.read_sql(query, data_warehouse_connection.conn)
        data_deserializor = DataDeserializor()
        deserialized_data = {}

        if len(dimensions) == 1:
            data_deserializor.multi_dim_deserialization(
                client_chart_metadata.fact_column,
                global_dim_columns,
                chart_data,
                deserialized_data,
                DataDeserializationLevel.LEVEL_1,
                is_dim_date_quarter,
            )
        else:
            data_deserializor.multi_dim_deserialization(
                client_chart_metadata.fact_column,
                global_dim_columns,
                chart_data,
                deserialized_data,
                DataDeserializationLevel.LEVEL_2,
                is_dim_date_quarter,
            )

        color_manager = ColorManager(catched_colors=catched_colors)
        chart_generator_manager = ChartGeneratorManager()

        chart_metadata = chart_generator_manager.chart_generating(
            chart_type=client_chart_metadata.chart_type,
            is_one_dim=len(dimensions) == 1,
            fact_col=client_chart_metadata.fact_column,
            deserialized_data=deserialized_data,
            color_manger=color_manager,
        )

        print(f"The chart_metadata is {chart_metadata}")

        metadata = {
            "chart_state": {
                "catched_color": color_manager.get_catched_color(),
                "client_chart_metadata": client_chart_metadata,
            }
        }

        if client_chart_metadata.chart_type == ConstantProvider.bar_chart_name():
            metadata["chart"] = BarChart(**chart_metadata)
        elif client_chart_metadata.chart_type == ConstantProvider.line_chart_name():
            metadata["chart"] = LineChart(**chart_metadata)
        elif client_chart_metadata.chart_type == ConstantProvider.pie_chart_name():
            metadata["chart"] = PieChart(**chart_metadata)
        elif client_chart_metadata.chart_type in (
            ConstantProvider.map_chart_name(),
            ConstantProvider.map_region_chart_name(),
        ):
            metadata["chart"] = MapChart(**chart_metadata)

        print(f"The returned data is: {metadata}")
        return metadata

    except Exception:
        print(traceback.format_exc())
        raise (Exception)
    finally:
        data_warehouse_connection.disconnect()


@router.post("/save-chart")
def save_chart(chart_metadata: ChartMetaData, chart_name: Annotated[str, Body()], user: User = Depends(get_current_user)):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        insert_query = OperationalDBConnection.get_postgres_sql(
            """
                INSERT INTO chart (user_id, chart_name, cached_chart, state, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
        )

        cursor.execute(
            insert_query,
            (
                user.user_id,
                chart_name,
                json.dumps(chart_metadata.chart.model_dump()),
                json.dumps(chart_metadata.chart_state.model_dump()),
                datetime.now(),
                datetime.now(),
            ),
        )

        connection.commit()
        return Response(status_code=200)
    except Exception:
        print(traceback.format_exc())
        if connection:
            connection.rollback()
            print("Transaction rolled back")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@router.delete("/{chart_id}")
def delete_chart(chart_id: str, user: User = Depends(get_current_user)):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        delete_query = OperationalDBConnection.get_postgres_sql(
            "DELETE FROM chart WHERE chart_id = %s and user_id = %s"
        )
        cursor.execute(delete_query, (chart_id, user.user_id))

        connection.commit()
        return Response(status_code=200)

    except Exception:
        print(traceback.format_exc())
        if connection:
            connection.rollback()
            print("Transaction rolled back")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@router.delete("/")
def delete_chart(user: User = Depends(get_current_user)):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        delete_query = OperationalDBConnection.get_postgres_sql("DELETE FROM chart where user_id = %s")
        cursor.execute(delete_query, (user.user_id,))

        connection.commit()
        return Response(status_code=200)

    except Exception:
        print(traceback.format_exc())
        if connection:
            connection.rollback()
            print("Transaction rolled back")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def cache_chart(charts, user_id: str):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        for chart in charts:
            update_query = OperationalDBConnection.get_postgres_sql(
                "UPDATE chart SET cached_chart = %s, updated_at = %s  WHERE chart_id = %s and user_id = %s"
            )
            cursor.execute(
                update_query,
                (
                    chart["id"],
                    datetime.now(),
                    chart["chart"],
                    user_id
                ),
            )

        connection.commit()
    except Exception:
        print(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@router.get("/", response_model=list[FetchedChartMetaData])
def get_all_charts(is_cached: bool = True, user: User = Depends(get_current_user)):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()

        select_query = OperationalDBConnection.get_postgres_sql("SELECT * FROM chart where user_id = %s")
        cursor.execute(select_query, (user.user_id,))

        charts = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        db_charts = [dict(zip(columns, chart)) for chart in charts]

        fetched_chart_data = []
        for chart in db_charts:
            if is_cached:
                chart_metadata = chart["cached_chart"]
            else:
                chart_metadata = preview_chart(
                    client_chart_metadata=ClientChartMetaData(
                        **chart["state"]["client_chart_metadata"]
                    ),
                    catched_colors=chart["state"]["catched_color"],
                ).get("chart")

            temp_chart_data = {
                "id": chart["chart_id"],
                "chartName": chart["chart_name"],
                "chartType": chart["state"]["client_chart_metadata"]["chart_type"],
                "chart": chart_metadata,
            }
            fetched_chart_data.append(FetchedChartMetaData(**temp_chart_data))

        if not is_cached:
            cache_chart(fetched_chart_data, user.user_id)

        return fetched_chart_data
    except Exception:
        print(traceback.format_exc())
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def fetch_cached_metric_result(metric: str):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        select_query = OperationalDBConnection.get_postgres_sql(
            "SELECT result FROM information_cached_metric WHERE metric = %s"
        )
        cursor.execute(select_query, (metric,))

        return cursor.fetchone()
    except Exception:
        print(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def cache_metric_result(metrics: dict):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        for metric in metrics:
            insert_query = OperationalDBConnection.get_postgres_sql(
                """
                INSERT INTO information_cached_metric (metric, result)
                VALUES (%s, %s)
                ON CONFLICT (metric)
                DO UPDATE SET result = EXCLUDED.result
                """
            )
            cursor.execute(
                insert_query,
                (
                    metric,
                    metrics[metric],
                ),
            )

        connection.commit()
    except Exception:
        print(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@router.get("/widgets", response_model=FetchedDataWidget)
def get_widgets(is_cached: bool = True):
    data_warehouse_connection = DataWarehouseConnection()
    try:
        data_warehouse_connection.connect()
        connection = data_warehouse_connection.conn
        cursor = connection.cursor()

        metrics = {
            "totalSalesAmount": QueryParserManager.total_sales_amount_query(),
            "totalOrders": QueryParserManager.total_orders_query(),
            "totalCustomers": QueryParserManager.total_customers_query(),
            "totalProducts": QueryParserManager.total_products_query(),
        }

        result = {}
        for metric in metrics.keys():
            cached_result = fetch_cached_metric_result(metric)
            if cached_result is not None and is_cached:
                result[metric] = cached_result[0]
                continue

            cursor.execute(metrics[metric])
            record = cursor.fetchone()
            result[metric] = record[0]

        cache_metric_result(result)

        return result

    except Exception:
        print(traceback.format_exc())
        raise (Exception)
    finally:
        if cursor:
            cursor.close()
        connection.close()


@router.get("/dashboards", response_model=DashBoard)
def get_dashboard(user: User = Depends(get_current_user)):
    return {"charts": get_all_charts(user=user), "fetchDataWidget": get_widgets()}


@router.get("/refresh", response_model=DashBoard)
def refresh_dashboard(user: User = Depends(get_current_user)):
    return {
        "charts": get_all_charts(is_cached=False, user=user),
        "fetchDataWidget": get_widgets(is_cached=False),
    }
