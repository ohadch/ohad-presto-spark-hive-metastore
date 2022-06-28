import os
from logging import getLogger

import pandas as pd
from hive_metastore_client.builders import ColumnBuilder

from utils.types import MetastoreDataTypes

FALLBACK_DATA_TYPE = MetastoreDataTypes.STRING

logger = getLogger(__name__)


def get_metastore_dtype(series: pd.Series) -> str:
    """
    Returns the primitive data type by the pandas series.
    @param series: The pandas series.
    @return: The primitive data type.
    """
    if len(series.dropna()) == 0:
        data_type = MetastoreDataTypes.STRING
    elif series.isna().sum() > 0:
        # If there are missing values, we fallback to string as Athena doesn't support missing values in CSV files.
        data_type = MetastoreDataTypes.STRING
    elif pd.api.types.is_bool_dtype(series) or all(series.apply(type) == bool):
        data_type = MetastoreDataTypes.BOOLEAN
    elif pd.api.types.is_int64_dtype(series) or all(series.apply(type) == int):
        data_type = MetastoreDataTypes.INT
    elif pd.api.types.is_numeric_dtype(series) or all(series.apply(type) == float):
        data_type = MetastoreDataTypes.DOUBLE
    elif pd.api.types.is_string_dtype(series):
        data_type = MetastoreDataTypes.STRING
    else:
        # Please note that we currently do not support datetime, timestamp, and date.
        # This is not because we can't indentify them, but because their deserialization
        # is different between different SerDe.
        logger.warning(
            f"Unknown pandas dtype: {series.dtype.name}, falling back to {FALLBACK_DATA_TYPE}."
        )
        data_type = FALLBACK_DATA_TYPE

    return data_type


def read_dataframe_by_file_path(file_path: str) -> pd.DataFrame:
    """
    Reads a dataframe from a file.
    :param file_path: The file path to read the dataframe from.
    :return: The dataframe.
    """
    extension = os.path.splitext(file_path)[1]

    if extension == ".csv":
        return pd.read_csv(file_path)
    elif extension == ".parquet":
        return pd.read_parquet(file_path)
    else:
        raise ValueError("File extension not supported.")


def get_hive_columns_by_dataframe(df: pd.DataFrame) -> List[Column]:
    """
    Returns the Hive columns by the pandas dataframe.
    :param df: The pandas dataframe.
    :return: The Hive columns.
    """
    return [
        ColumnBuilder(
            name=col,
            type=get_metastore_dtype(df[col])
        ).build()
        for col
        in df.columns
    ]