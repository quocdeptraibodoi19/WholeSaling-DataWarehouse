from pydantic import BaseModel
from uuid import UUID
from datetime import datetime


class State(BaseModel):
    fact_name: str
    dim_names: list[str]
    cached_query: str
    cached_colors: list[str]


class ChartMetaData(BaseModel):
    chart_id: UUID
    chart_name: str
    state: State
    created_at: datetime
    updated_at: datetime

class ClientDimensionMetaData(BaseModel):
    dim_name: str
    dim_column: str
    dim_key: str
    ref_fact_key: str

class ClientChartMetaData(BaseModel):
    chart_type: str
    fact_name: str
    fact_column: str
    dimensions: list[ClientDimensionMetaData]
