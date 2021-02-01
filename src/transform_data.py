# pyright: reportGeneralTypeIssues=false
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower, ceil
from functools import reduce
from .csv_schema import csv_schema, amount_cols
from .connect import connect


def good_standing(df: DataFrame) -> DataFrame:
    """
    Filters entries with a charged off loan_status
    """
    return df.filter(lower(df.loan_status) != "charged off")


def known_purpose(df: DataFrame) -> DataFrame:
    """
    Filters entries with a purpose of other
    """
    return df.filter(lower(df.purpose) != "other")


def high_credit(df: DataFrame, score: int = 700) -> DataFrame:
    """
    Filters entries with a lower fico score than the one given

        Parameters:
            df (DataFrame): A pyspark DataFrame
            score (int): minimum acceptable fico score
    """
    return df.filter(df.fico_range_low >= score)


def round_up_cents(df: DataFrame, column: str, precision: int = 2) -> DataFrame:
    """
    Rounds up single column to a given precision and returns a dataframe

        Parameters:
            df (DataFrame): A pyspark DataFrame
            column (str): The column that the transformation should be applied to
            precision (int): digits after the decimal point the mapping will round to (default: 2)
    """
    return df.withColumn(column, ceil(df[column] * 10 ** precision) / 10 ** precision)


def round_up_cents_cols(df: DataFrame, columns: list, precision: int = 2) -> DataFrame:
    """
    Rounds up given columns to a given precision and returns a dataframe

        Parameters:
            df (DataFrame): A pyspark DataFrame
            columns (str): The columns that the transformation should be applied to
            precision (int): digits after the decimal point the mapping will round to (default: 2)
    """
    return reduce(
        lambda result, column: round_up_cents(result, column, precision), columns, df
    )


def transform_data(df: DataFrame, columns):
    """
    Applies all of the transforms in this module on the dataframe

        Parameters:
            df (DataFrame): A pyspark DataFrame
            columns (str): The columns that should be rounded up
    """
    df = df.transform(good_standing).transform(known_purpose).transform(high_credit)
    return round_up_cents_cols(df, columns)


if __name__ == "__main__":
    pass
