from pydantic import BaseModel
from .chart_metadata import ClientChartMetaData


class ChartBase(BaseModel):
    labels: list[str]


class ChartDataset(BaseModel):
    label: str


class BarChartDataset(ChartDataset):
    backgroundColor: str
    data: list[int | float]


class LineChartDataset(ChartDataset):
    backgroundColor: str
    data: list[int | float]


class PieChartDataset(ChartDataset):
    backgroundColor: list[str]
    data: list[int | float]


class MapchartDataset(ChartDataset):
    label: str
    data: int | float


class BarChart(ChartBase):
    datasets: list[BarChartDataset]


class LineChart(ChartBase):
    datasets: list[LineChartDataset]


class PieChart(ChartBase):
    datasets: list[PieChartDataset]


class MapChart(ChartBase):
    datasets: list[MapchartDataset]


class ChartState(BaseModel):
    catched_color: list[str]
    client_chart_metadata: ClientChartMetaData


class ChartMetaData(BaseModel):
    chart: MapChart | PieChart | BarChart | LineChart
    chart_state: ChartState
