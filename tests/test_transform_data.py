import pytest
from pyspark.sql import SparkSession
from src.transform_data import good_standing, known_purpose, high_credit


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Transforms").master("local").getOrCreate()
    yield spark
    spark.stop()


class TestGoodStanding:
    @pytest.fixture(scope="class")
    def schema(self):
        return ["id", "loan_status"]

    @pytest.fixture
    def data(self):
        return [(0, "Charged Off"), (1, "Fully Paid"), (2, "charged off")]

    @pytest.fixture
    def df(self, spark, data, schema):
        return spark.createDataFrame(data, schema)

    def test_filters_charged_off(self, df):
        assert good_standing(df).filter(df["loan_status"] == "Charged Off").count() == 0

    def test_only_filters_charged_off(self, df, spark, schema):
        assert (
            good_standing(df).collect()
            == spark.createDataFrame([(1, "Fully Paid")], schema).collect()
        )

    def test_filters_charged_off_case_insensitive(self, df):
        assert good_standing(df).filter(df["loan_status"] == "charged off").count() == 0


class TestKnownPurpose:
    @pytest.fixture(scope="class")
    def schema(self):
        return ["id", "purpose"]

    @pytest.fixture
    def data(self):
        return [(0, "mortgage"), (1, "other"), (2, "Other")]

    @pytest.fixture
    def df(self, spark, data, schema):
        return spark.createDataFrame(data, schema)

    def test_filters_other(self, df):
        assert known_purpose(df).filter(df["purpose"] == "other").count() == 0

    def test_filters_other_case_insensitive(self, df):
        assert known_purpose(df).filter(df["purpose"] == "other").count() == 0

    def test_only_filters_other(self, df, spark, schema):
        assert (
            known_purpose(df).collect()
            == spark.createDataFrame([(0, "mortgage")], schema).collect()
        )


class TestHighCredit:
    @pytest.fixture(scope="class")
    def schema(self):
        return ["id", "last_fico_range_low"]

    @pytest.fixture
    def data(self):
        return [(0, 650), (1, 750), (2, 700), (3, 600)]

    @pytest.fixture
    def df(self, spark, data, schema):
        return spark.createDataFrame(data, schema)

    def test_filters_low_credit(self, df):
        assert high_credit(df).filter(df["last_fico_range_low"] < 700).count() == 0

    def test_only_filters_low_credit(self, df, spark, schema):
        assert (
            high_credit(df).collect()
            == spark.createDataFrame([(1, 750), (2, 700)], schema).collect()
        )
