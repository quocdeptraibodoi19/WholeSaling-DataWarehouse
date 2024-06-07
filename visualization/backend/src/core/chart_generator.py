import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from .color_manager import ColorManager
from src.constants import ConstantProvider

from abc import ABC, abstractmethod


class ChartGeneratingStrategy(ABC):
    @abstractmethod
    def chart_generating(
        self, fact_col: str, deserialized_data: dict, color_manager: ColorManager
    ):
        pass


class OneDimChartGeneratingStrategy(ChartGeneratingStrategy):
    def chart_generating(
        self, fact_col: str, deserialized_data: dict, color_manager: ColorManager
    ):
        dim_data = deserialized_data.keys()
        fact_data = []
        for dim in dim_data:
            fact_data.append(deserialized_data[dim][fact_col])

        return {
            "labels": dim_data,
            "datasets": [
                {
                    "label": fact_col,
                    "backgroundColor": color_manager.increment(),
                    "data": fact_data,
                }
            ],
        }


class OneDimPieChartGenerattingStrategy(ChartGeneratingStrategy):
    def chart_generating(
        self,
        fact_col: str,
        deserialized_data: dict,
        color_manager: ColorManager,
    ):
        dim_data = deserialized_data.keys()
        fact_data = []
        for dim in dim_data:
            fact_data.append(deserialized_data[dim][fact_col])

        return {
            "labels": dim_data,
            "datasets": [
                {
                    "label": fact_col,
                    "backgroundColor": color_manager.generate_colors(len(dim_data)),
                    "data": fact_data,
                }
            ],
        }


class MapChartGeneratingStrategy(ChartGeneratingStrategy):
    def chart_generating(
        self,
        fact_col: str,
        deserialized_data: dict,
        color_manager: ColorManager,
    ):
        country_dim_data = deserialized_data.keys()
        fact_data = []
        for dim in country_dim_data:
            fact_data.append(deserialized_data[dim][fact_col])

        datasets = list(
            map(
                lambda element: {"label": element[0], "data": element[1]},
                zip(country_dim_data, fact_data),
            )
        )
        return {"labels": [], "datasets": datasets}


# The second dim is always lying inside, the fist dim will lie outside
# That means, the dimdate is always the first dim => The frontend will send out it first to the backend.
class TwoDimChartGeneratingStrategy(ChartGeneratingStrategy):
    def chart_generating(
        self,
        fact_col: str,
        deserialized_data: dict,
        color_manager: ColorManager,
    ):

        first_dim_data, sec_dim_data = [], []

        first_dim_data = deserialized_data.keys()
        sec_dim_data = deserialized_data[first_dim_data[0]].keys()

        datasets = []
        for sec_dim in sec_dim_data:
            temp_fact_data = []
            temp_dict = {}
            for first_dim in first_dim_data:
                temp_fact_data.append(deserialized_data[first_dim][sec_dim][fact_col])
                temp_dict = {
                    "label": sec_dim,
                    "backgroundColor": color_manager.increment(),
                    "data": temp_fact_data,
                }
                datasets.append(temp_dict)

        return {"labels": first_dim_data, "datasets": datasets}


class ChartGenerator:
    def __init__(
        self, chart_generating_strategy: ChartGeneratingStrategy = None
    ) -> None:
        self.chart_generating_strategy = chart_generating_strategy

    def set_strategy(self, chart_generating_strategy: ChartGeneratingStrategy):
        self.chart_generating_strategy = chart_generating_strategy

    def chart_generating(
        self, fact_col: str, deserialized_data: dict, color_manger: ColorManager
    ):
        return self.chart_generating_strategy.chart_generating(
            fact_col=fact_col,
            deserialized_data=deserialized_data,
            color_manager=color_manger,
        )


class ChartGeneratorManager:
    def __init__(self) -> None:
        self.chart_generator = ChartGenerator()

    def chart_generating(
        self,
        chart_type: str,
        is_one_dim: bool,
        fact_col: str,
        deserialized_data: dict,
        color_manger: ColorManager = None,
    ):
        color_manger = ColorManager() if color_manger is None else color_manger
        if is_one_dim:
            if chart_type in (
                ConstantProvider.bar_chart_name(),
                ConstantProvider.line_chart_name(),
            ):
                self.chart_generator.set_strategy(OneDimChartGeneratingStrategy())
            elif chart_type == ConstantProvider.pie_chart_name():
                self.chart_generator.set_strategy(OneDimChartGeneratingStrategy())
            elif chart_type == ConstantProvider.map_chart_name():
                self.chart_generator.set_strategy(MapChartGeneratingStrategy())
        else:
            self.chart_generator.set_strategy(TwoDimChartGeneratingStrategy())

        return self.chart_generator.chart_generating(
            fact_col=fact_col,
            deserialized_data=deserialized_data,
            color_manger=color_manger,
        )
