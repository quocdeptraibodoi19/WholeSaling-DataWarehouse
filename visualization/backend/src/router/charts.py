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

from core.connections import DataWarehouseConnection, OperationalDBConnection
from core.data_processor import DataDeserializor
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
    ChartState,
    FetchedChartMetaData,
    FetchedDataWidget,
)
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
        is_dim_date_quarter, is_other_dims = False, False
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
                is_other_dims = True

            global_dim_columns += dim_columns

            dim_name = dimension.dim_name
            dim_key = dimension.dim_key
            ref_fact_key = dimension.ref_fact_key

            dimensions.append(SelectedDim(dim_name, dim_columns, dim_key, ref_fact_key))

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

        is_one_dim = False
        if is_dim_date_quarter and not is_other_dims:
            data_deserializor.multi_dim_deserialization(
                client_chart_metadata.fact_column,
                global_dim_columns,
                chart_data,
                deserialized_data,
                1,
            )
            is_one_dim = True
        else:
            data_deserializor.multi_dim_deserialization(
                client_chart_metadata.fact_column,
                global_dim_columns,
                chart_data,
                deserialized_data,
                2,
            )

        color_manager = ColorManager(catched_colors=catched_colors)
        is_one_dim = is_one_dim or len(global_dim_columns) == 1
        chart_generator_manager = ChartGeneratorManager()

        chart_metadata = chart_generator_manager.chart_generating(
            chart_type=client_chart_metadata.chart_type,
            is_one_dim=is_one_dim,
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
        elif client_chart_metadata.chart_type == ConstantProvider.map_chart_name():
            metadata["chart"] = MapChart(**chart_metadata)

        print(f"The returned data is: {metadata}")
        return metadata

    except Exception:
        print(traceback.format_exc())
        raise (Exception)
    finally:
        data_warehouse_connection.disconnect()


@router.post("/save-chart")
def save_chart(chart_state: ChartState, chart_name: Annotated[str, Body()]):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        insert_query = OperationalDBConnection.get_postgres_sql(
            """
                INSERT INTO chart (chart_name, state, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
            """
        )

        cursor.execute(
            insert_query,
            (
                chart_name,
                json.dumps(chart_state.model_dump()),
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


@router.delete("/charts/{chart_id}")
def delete_chart(chart_id: str):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        delete_query = OperationalDBConnection.get_postgres_sql(
            "DELETE FROM chart WHERE chart_id = %s"
        )
        cursor.execute(delete_query, (chart_id,))

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


@router.get("/charts", response_model=list[FetchedChartMetaData])
def get_all_charts():
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()

        select_query = OperationalDBConnection.get_postgres_sql("SELECT * FROM chart")
        cursor.execute(select_query)

        charts = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        db_charts = [dict(zip(columns, chart)) for chart in charts]

        fetched_chart_data = []
        for chart in db_charts:
            chart_metadata = preview_chart(
                client_chart_metadata=ClientChartMetaData(
                    **chart["state"]["client_chart_metadata"]
                ),
                catched_colors=chart["state"]["catched_color"],
            )

            temp_chart_data = {
                "id": chart["chart_id"],
                "chartName": chart["chart_name"],
                "chartType": chart["state"]["client_chart_metadata"]["chart_type"],
                "chart": chart_metadata.get("chart"),
            }
            fetched_chart_data.append(FetchedChartMetaData(**temp_chart_data))

        return fetched_chart_data
    except Exception:
        print(traceback.format_exc())
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@router.get("/data-fetch", response_model=FetchedDataWidget)
def data_fetching():
    data_warehouse_connection = DataWarehouseConnection()
    try:
        data_warehouse_connection.connect()
        connection = data_warehouse_connection.conn
        cursor = connection.cursor()

        queries = [
            QueryParserManager.total_customers_query(),
            QueryParserManager.total_sales_amount_query(),
            QueryParserManager.total_orders_query(),
            QueryParserManager.total_products_query(),
        ]

        result = {}
        for query in queries:
            cursor.execute(query)
            record = cursor.fetchone()
            result[cursor.description[0][0]] = record[0]

        return result

    except Exception:
        print(traceback.format_exc())
        raise (Exception)
    finally:
        if cursor:
            cursor.close()
        connection.close()
