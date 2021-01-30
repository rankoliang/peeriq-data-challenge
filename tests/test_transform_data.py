import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    DoubleType,
    IntegerType,
    StringType,
)
from src.transform_data import (
    good_standing,
    known_purpose,
    high_credit,
    round_up_cents,
    round_up_cents_cols,
)


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Transforms").master("local").getOrCreate()
    yield spark
    spark.stop()


class TestTransform:
    @pytest.fixture
    def df(self, spark, data, schema):
        return spark.createDataFrame(data, schema)


class TestGoodStanding(TestTransform):
    @pytest.fixture(scope="class")
    def schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("loan_status", StringType(), True),
            ]
        )

    @pytest.fixture
    def data(self):
        return [(0, "Charged Off"), (1, "Fully Paid"), (2, "charged off")]

    def test_filters_charged_off(self, df):
        assert good_standing(df).filter(df["loan_status"] == "Charged Off").count() == 0

    def test_only_filters_charged_off(self, df, spark, schema):
        assert (
            good_standing(df).collect()
            == spark.createDataFrame([(1, "Fully Paid")], schema).collect()
        )

    def test_filters_charged_off_case_insensitive(self, df):
        assert good_standing(df).filter(df["loan_status"] == "charged off").count() == 0


class TestKnownPurpose(TestTransform):
    @pytest.fixture(scope="class")
    def schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("purpose", StringType(), True),
            ]
        )

    @pytest.fixture
    def data(self):
        return [(0, "mortgage"), (1, "other"), (2, "Other")]

    def test_filters_other(self, df):
        assert known_purpose(df).filter(df["purpose"] == "other").count() == 0

    def test_filters_other_case_insensitive(self, df):
        assert known_purpose(df).filter(df["purpose"] == "other").count() == 0

    def test_only_filters_other(self, df, spark, schema):
        assert (
            known_purpose(df).collect()
            == spark.createDataFrame([(0, "mortgage")], schema).collect()
        )


class TestHighCredit(TestTransform):
    @pytest.fixture(scope="class")
    def schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("last_fico_range_low", IntegerType(), True),
            ]
        )

    @pytest.fixture
    def data(self):
        return [(0, 650), (1, 750), (2, 700), (3, 600)]

    def test_filters_low_credit(self, df):
        assert high_credit(df).filter(df["last_fico_range_low"] < 700).count() == 0

    def test_only_filters_low_credit(self, df, spark, schema):
        assert (
            high_credit(df).collect()
            == spark.createDataFrame([(1, 750), (2, 700)], schema).collect()
        )


class TestRoundUpCents(TestTransform):
    @pytest.fixture(scope="class")
    def schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("amount", DoubleType(), True),
            ]
        )

    @pytest.fixture
    def data(self):
        return [(0, 123.1234), (1, 100.0), (2, 25.00), (3, 10.016)]

    def test_rounds_up_cents(self, df, spark, schema):
        assert (
            round_up_cents(df, "amount", 2).collect()
            == spark.createDataFrame(
                [(0, 123.13), (1, 100.00), (2, 25.00), (3, 10.02)], schema
            ).collect()
        )

    def test_rounds_up_cents_changed(self, df, spark, schema):
        assert round_up_cents(df, "amount", 2).collect() != df.collect()


class TestRoundUpCentsCols(TestTransform):
    @pytest.fixture(scope="class")
    def schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("amount", DoubleType(), True),
                StructField("other_amount", DoubleType(), True),
            ]
        )

    @pytest.fixture
    def data(self):
        return [
            (0, 123.1234, 10.016),
            (1, 100.0, 25.00),
            (2, 25.00, 100.0),
            (3, 10.016, 123.1234),
        ]

    def test_rounds_up_cents_cols(self, df, spark, schema):
        assert (
            round_up_cents_cols(df, ["amount", "other_amount"], 2).collect()
            == spark.createDataFrame(
                [
                    (0, 123.13, 10.02),
                    (1, 100.00, 25.00),
                    (2, 25.00, 100.00),
                    (3, 10.02, 123.13),
                ],
                schema,
            ).collect()
        )

    def test_rounds_up_cents_cols_changed(self, df, spark, schema):
        assert (
            round_up_cents_cols(df, ["amount", "other_amount"], 2).collect()
            != df.collect()
        )
