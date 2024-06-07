import pandas as pd


class DataDeserializor:
    def __init__(self) -> None:
        pass

    @staticmethod
    def generate_combined_key(row, dim_cols, wishing_level):
        if len(dim_cols) == 1:
            first_level_key = row[dim_cols[0]]
            second_level_key = None
        elif wishing_level == 1:
            first_level_key = " - ".join(str(row[dim_col]) for dim_col in dim_cols)
            second_level_key = None
        else:
            if wishing_level >= len(dim_cols):
                wishing_level = len(dim_cols) - 1
            first_level_key = row[dim_cols[0]]
            second_level_key = " - ".join(
                str(row[dim_col]) for dim_col in dim_cols[1 : wishing_level + 1]
            )
        return first_level_key, second_level_key

    @staticmethod
    def nested_dict_update(d, keys, value, fact_col):
        if keys[0] not in d:
            if keys[1] is None:
                d[keys[0]] = {fact_col: 0}
            else:
                d[keys[0]] = {}
        if keys[1] is None:
            d[keys[0]][fact_col] += value
        else:
            if keys[1] not in d[keys[0]]:
                d[keys[0]][keys[1]] = {fact_col: 0}
            d[keys[0]][keys[1]][fact_col] += value

    @staticmethod
    def multi_dim_deserialization(
        fact_col: str,
        dim_cols: list,
        data: pd.DataFrame,
        nested_dict: dict,
        maximum_wishing_level: int,
    ):
        for _, row in data.iterrows():
            first_level_key, second_level_key = DataDeserializor.generate_combined_key(
                row, dim_cols, maximum_wishing_level
            )
            value = row[fact_col]
            DataDeserializor.nested_dict_update(
                nested_dict, [first_level_key, second_level_key], value, fact_col
            )
