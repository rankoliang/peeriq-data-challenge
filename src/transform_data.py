# pyright: reportGeneralTypeIssues=false
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower


def good_standing(df: DataFrame) -> DataFrame:
    return df.filter(lower(df.loan_status) != "charged off")


def known_purpose(df: DataFrame) -> DataFrame:
    return df.filter(lower(df.purpose) != "other")
