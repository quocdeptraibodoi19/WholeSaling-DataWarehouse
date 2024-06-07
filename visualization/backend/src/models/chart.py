from pydantic import BaseModel


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
