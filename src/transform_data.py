# pyright: reportGeneralTypeIssues=false
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower


def good_standing(df: DataFrame) -> DataFrame:
    return df.filter(df.loan_status != "Charged Off")


def known_purpose(df: DataFrame) -> DataFrame:
    return df.filter(df["purpose"] != "other")
