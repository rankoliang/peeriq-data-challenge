# pyright: reportGeneralTypeIssues=false
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower, ceil


def good_standing(df: DataFrame) -> DataFrame:
    return df.filter(lower(df.loan_status) != "charged off")


def known_purpose(df: DataFrame) -> DataFrame:
    return df.filter(lower(df.purpose) != "other")


def high_credit(df: DataFrame) -> DataFrame:
    return df.filter(df.last_fico_range_low >= 700)


def round_up_cents(df: DataFrame, column: str, precision: int = 2) -> DataFrame:
    return df.withColumn(column, ceil(df[column] * 10 ** precision) / 10 ** precision)
