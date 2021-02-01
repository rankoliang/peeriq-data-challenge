# pyright: reportGeneralTypeIssues=false
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lower, ceil
from functools import reduce
import os
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)
import argparse


def connect(target, args):
    SERVER = args.server
    PORT = args.port
    DB_NAME = args.db_name
    DB_USER = args.db_user
    DB_PASSWORD = args.db_password
    TABLE_NAME = args.table_name

    return (
        target.format("jdbc")
        .option("url", f"jdbc:postgresql://{SERVER}:{PORT}/{DB_NAME}")
        .option("dbtable", TABLE_NAME)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
    )


csv_schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("loan_amnt", DoubleType(), True),
        StructField("funded_amnt", DoubleType(), True),
        StructField("funded_amnt_inv", DoubleType(), True),
        StructField("term", StringType(), True),
        StructField("int_rate", StringType(), True),
        StructField("installment", DoubleType(), True),
        StructField("grade", StringType(), True),
        StructField("sub_grade", StringType(), True),
        StructField("emp_length", StringType(), True),
        StructField("home_ownership", StringType(), True),
        StructField("annual_inc", DoubleType(), True),
        StructField("verification_status", StringType(), True),
        StructField("issue_d", StringType(), True),
        StructField("loan_status", StringType(), True),
        StructField("desc", StringType(), True),
        StructField("purpose", StringType(), True),
        StructField("title", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("addr_state", StringType(), True),
        StructField("dti", DoubleType(), True),
        StructField("delinq_2yrs", IntegerType(), True),
        StructField("earliest_cr_line", StringType(), True),
        StructField("fico_range_low", IntegerType(), True),
        StructField("fico_range_high", IntegerType(), True),
        StructField("inq_last_6mths", IntegerType(), True),
        StructField("mths_since_last_delinq", IntegerType(), True),
        StructField("mths_since_last_record", IntegerType(), True),
        StructField("open_acc", IntegerType(), True),
        StructField("pub_rec", IntegerType(), True),
        StructField("revol_bal", DoubleType(), True),
        StructField("revol_util", StringType(), True),
        StructField("total_acc", IntegerType(), True),
        StructField("initial_list_status", StringType(), True),
        StructField("out_prncp", IntegerType(), True),
        StructField("out_prncp_inv", IntegerType(), True),
        StructField("total_pymnt", DoubleType(), True),
        StructField("total_pymnt_inv", DoubleType(), True),
        StructField("total_rec_prncp", DoubleType(), True),
        StructField("total_rec_int", DoubleType(), True),
        StructField("total_rec_late_fee", DoubleType(), True),
        StructField("recoveries", DoubleType(), True),
        StructField("collection_recovery_fee", DoubleType(), True),
        StructField("last_pymnt_d", StringType(), True),
        StructField("last_pymnt_amnt", DoubleType(), True),
        StructField("next_pymnt_d", StringType(), True),
        StructField("last_credit_pull_d", StringType(), True),
        StructField("last_fico_range_high", IntegerType(), True),
        StructField("last_fico_range_low", IntegerType(), True),
        StructField("collections_12_mths_ex_med", IntegerType(), True),
        StructField("mths_since_last_major_derog", IntegerType(), True),
        StructField("policy_code", IntegerType(), True),
        StructField("application_type", StringType(), True),
    ]
)

amount_cols = [
    "loan_amnt",
    "funded_amnt",
    "funded_amnt_inv",
    "revol_bal",
    "total_pymnt",
    "total_pymnt_inv",
    "total_rec_prncp",
    "total_rec_int",
    "recoveries",
    "collection_recovery_fee",
    "last_pymnt_amnt",
]


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


def pipe(df, *funcs):
    return reduce(lambda dataframe, func: func(dataframe), funcs, df)


def transform_data(df: DataFrame, columns):
    """
    Applies all of the transforms in this module on the dataframe

        Parameters:
            df (DataFrame): A pyspark DataFrame
            columns (str): The columns that should be rounded up
    """
    df = pipe(df, good_standing, known_purpose, high_credit)
    return round_up_cents_cols(df, columns)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-source", help="URI where the loans data are saved, such as an S3 bucket"
    )
    parser.add_argument(
        "--server", help="server where postgresql is running, such as an RDS hostname"
    )
    parser.add_argument("--port", help="port used to connect to the postgresql server")
    parser.add_argument(
        "--db-name", help="name of the database where the data will be loaded to"
    )
    parser.add_argument("--db-user", help="username used to access the server")
    parser.add_argument("--db-password", help="password associated with the username")
    parser.add_argument(
        "--table-name", help="name of the table where data will be loaded to"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    with (SparkSession.builder.appName("db_loader").getOrCreate()) as spark:
        loan_file = args.data_source

        loans = (
            spark.read.schema(csv_schema)
            .load(loan_file, format="csv", header=True)
            .cache()
        )

        loans = transform_data(loans, amount_cols)

        connect(loans.write.mode("append"), args).save()
