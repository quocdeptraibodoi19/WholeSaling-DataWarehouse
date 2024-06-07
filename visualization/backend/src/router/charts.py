import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from fastapi import APIRouter, Response, status, Path, Query, Depends, Body

import pandas as pd

from core.connections import DataWarehouseConnection, OperationalDBConnection
from core.data_processor import DataDeserializor
from core.chart_generator import ChartGeneratorManager
from core.query_parser import SelectedFact, SelectedDim, QueryParserManager
from models.chart_metadata import ClientChartMetaData
from models.chart import BarChart, PieChart, LineChart, MapChart
from src.constants import ConstantProvider

router = APIRouter(prefix="/charts", tags=["charts"])


@router.post("/preview")
def preview_chart(client_chart_metadata: ClientChartMetaData):
    data_warehouse_connection = DataWarehouseConnection()
    # try:
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

    is_one_dim = is_one_dim or len(global_dim_columns) == 1
    chart_generator_manager = ChartGeneratorManager()
    chart_metadata = chart_generator_manager.chart_generating(
        chart_type=client_chart_metadata.chart_type,
        is_one_dim=is_one_dim,
        fact_col=client_chart_metadata.fact_column,
        deserialized_data=deserialized_data,
    )

    print(f"The chart_metadata is {chart_metadata}")

    if client_chart_metadata.chart_type == ConstantProvider.bar_chart_name():
        return BarChart(**chart_metadata)
    elif client_chart_metadata.chart_type == ConstantProvider.line_chart_name():
        return LineChart(**chart_metadata)
    elif client_chart_metadata.chart_type == ConstantProvider.pie_chart_name():
        return PieChart(**chart_metadata)
    elif client_chart_metadata.chart_type == ConstantProvider.map_chart_name():
        return MapChart(**chart_metadata)


# except:
#     return Response(status_code=status.HTTP_400_BAD_REQUEST)
# finally:
#     data_warehouse_connection.disconnect()
