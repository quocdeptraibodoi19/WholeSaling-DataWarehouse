import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from abc import ABC, abstractmethod

from src.constants import ConstantProvider


class SelectedDim:
    def __init__(
        self,
        dim_name: str,
        dim_columns: list[str],
        dim_key: str,
        ref_fact_key: str,
        dim_condition: list[str] = None,
    ):
        self._dim_name = dim_name
        self._dim_columns = dim_columns
        self._dim_key = dim_key
        self._ref_fact_key = ref_fact_key

        where_condition = self._process_raw_dim_condition(dim_condition)
        self._where_coditions = [] if where_condition is None else [where_condition]

        if self._dim_name in ConstantProvider.SCD_dims_list():
            self._where_coditions.append(
                ConstantProvider.valid_dim_condition(self._dim_name)
            )

    def _process_raw_dim_condition(self, dim_condition: list[dict]):
        if dim_condition == []:
            return None
        conditions = []
        for condition in dim_condition:
            temp_conditions = []
            for condition_key in condition.keys():
                if type(condition[condition_key]) == str:
                    temp_conditions.append(
                        f"{condition_key}='{condition[condition_key]}'"
                    )
                else:
                    temp_conditions.append(
                        f"{condition_key}={condition[condition_key]}"
                    )
            conditions.append(f'( {" and ".join(temp_conditions)} )')
        return f'( {" or ".join(conditions)} )'

    @property
    def dim_name(self) -> str:
        return self._dim_name

    @property
    def dim_columns(self) -> str:
        return self._dim_columns

    @property
    def dim_key(self) -> str:
        return self._dim_key

    @property
    def ref_fact_key(self) -> str:
        return self._ref_fact_key

    @property
    def where_coditions(self) -> list[str]:
        return self._where_coditions


class SelectedFact:
    def __init__(
        self,
        fact_name: str,
        fact_column: str,
        selected_dims: list[SelectedDim],
    ):
        self._fact_name = fact_name
        self._fact_column = fact_column
        self._selected_dims = selected_dims

    @property
    def fact_name(self) -> str:
        return self._fact_name

    @property
    def fact_column(self) -> str:
        return self._fact_column

    @property
    def selected_dims(self) -> list[SelectedDim]:
        return self._selected_dims


class ParsingStrategy(ABC):
    @abstractmethod
    def parse_query(self, selected_fact: SelectedFact):
        pass


class SimpleFactDimStrategy(ParsingStrategy):
    def parse_query(self, selected_fact: SelectedFact):
        fact_name = selected_fact.fact_name
        fact_column = selected_fact.fact_column

        selected_dim = selected_fact.selected_dims[0]
        dim_name = selected_dim.dim_name
        dim_columns = selected_dim.dim_columns
        dim_key = selected_dim.dim_key
        fact_key = selected_dim.ref_fact_key
        where_conditions = (
            f" WHERE {' AND '.join(selected_dim.where_coditions)} "
            if len(selected_dim.where_coditions) > 0
            else " "
        )

        fact_kpi_sale_amount = ConstantProvider.fact_kpi_sale_amount()
        fact_kpi_quantity = ConstantProvider.fact_kpi_quantity()

        common_query = (
            f"{','.join(dim_columns)} FROM {fact_name} "
            f"INNER JOIN {dim_name} "
            f"ON {fact_name}.{fact_key} = {dim_name}.{dim_key} "
        )
        common_query += where_conditions + f"GROUP BY {','.join(dim_columns)}"

        if fact_column == fact_kpi_sale_amount:
            return (
                f"SELECT SUM(sales_amount) AS `{fact_kpi_sale_amount}`, " + common_query
            )
        elif fact_column == fact_kpi_quantity:
            return f"SELECT COUNT(*) AS `{fact_kpi_quantity}`, " + common_query


class TwoDimFactStrategy(ParsingStrategy):
    def parse_query(self, selected_fact: SelectedFact):
        selected_first_dim = selected_fact.selected_dims[0]
        selected_sec_dim = selected_fact.selected_dims[1]

        fact_name = selected_fact.fact_name
        fact_column = selected_fact.fact_column
        first_fact_key = selected_first_dim.ref_fact_key
        sec_fact_key = selected_sec_dim.ref_fact_key

        first_dim_name = selected_first_dim.dim_name
        first_dim_columns = selected_first_dim.dim_columns
        first_dim_key = selected_first_dim.dim_key
        first_dim_conditions = (
            ""
            if len(selected_first_dim.where_coditions) == 0
            else " AND ".join(selected_first_dim.where_coditions)
        )

        sec_dim_name = selected_sec_dim.dim_name
        sec_dim_columns = selected_sec_dim.dim_columns
        sec_dim_key = selected_sec_dim.dim_key
        sec_dim_conditions = (
            ""
            if len(selected_sec_dim.where_coditions) == 0
            else " AND ".join(selected_sec_dim.where_coditions)
        )

        where_conditions = " "
        if (first_dim_conditions, sec_dim_conditions) != ("", ""):
            where_conditions = (
                " WHERE " + first_dim_conditions + " " + sec_dim_conditions + " "
            )

        fact_kpi_sale_amount = ConstantProvider.fact_kpi_sale_amount()
        fact_kpi_quantity = ConstantProvider.fact_kpi_quantity()

        common_query = (
            f"{','.join(first_dim_columns)}, {','.join(sec_dim_columns)} FROM {fact_name} "
            f"INNER JOIN {first_dim_name} ON {fact_name}.{first_fact_key} = {first_dim_name}.{first_dim_key} "
            f"INNER JOIN {sec_dim_name} ON {fact_name}.{sec_fact_key} = {sec_dim_name}.{sec_dim_key} "
        )

        common_query += (
            where_conditions
            + f"GROUP BY {','.join(first_dim_columns)}, {','.join(sec_dim_columns)}"
        )

        if fact_column == fact_kpi_sale_amount:
            return (
                f"SELECT SUM(sales_amount) AS `{fact_kpi_sale_amount}`, " + common_query
            )
        elif fact_column == fact_kpi_quantity:
            return f"SELECT COUNT(*) AS `{fact_kpi_quantity}`, " + common_query


class QueryParser:
    def __init__(self, strategy: ParsingStrategy = None) -> None:
        self.strategy = strategy

    def set_strategy(self, strategy: ParsingStrategy) -> None:
        self.strategy = strategy

    def parse_query(self, selected_fact: SelectedFact):
        return self.strategy.parse_query(selected_fact)


class QueryParserManager:
    def __init__(self) -> None:
        self.query_parser = QueryParser()

    def parse_query(
        self,
        selected_fact: SelectedFact,
    ):
        if len(selected_fact.selected_dims) == 1:
            self.query_parser.set_strategy(SimpleFactDimStrategy())
            return self.query_parser.parse_query(selected_fact=selected_fact)

        elif len(selected_fact.selected_dims) == 2:
            self.query_parser.set_strategy(TwoDimFactStrategy())
            return self.query_parser.parse_query(selected_fact=selected_fact)

    @staticmethod
    def total_orders_query():
        return ConstantProvider.total_orders_query()

    @staticmethod
    def total_customers_query():
        return ConstantProvider.total_customers_query()

    @staticmethod
    def total_sales_amount_query():
        return ConstantProvider.total_sales_amount_query()

    @staticmethod
    def total_products_query():
        return ConstantProvider.total_products_query()
