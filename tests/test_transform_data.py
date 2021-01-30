import pytest
from pyspark.sql import SparkSession
from src.transform_data import good_standing, known_purpose


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Transforms").master("local").getOrCreate()
    yield spark
    spark.stop()


class TestGoodStanding:
    @pytest.fixture
    def data(self):
        return [(0, "Charged Off"), (1, "Fully Paid")]

    @pytest.fixture
    def df(self, spark, data):
        return spark.createDataFrame(data, ["id", "loan_status"])

    def test_filters_charged_off(self, df):
        assert good_standing(df).filter(df["loan_status"] == "Charged Off").count() == 0

    def test_only_filters_charged_off(self, df):
        assert good_standing(df).count() == 1


class TestKnownPurpose:
    @pytest.fixture
    def data(self):
        return [(0, "mortgage"), (1, "other")]

    @pytest.fixture
    def df(self, spark, data):
        return spark.createDataFrame(data, ["id", "purpose"])

    def test_filters_charged_off(self, df):
        assert known_purpose(df).filter(df["purpose"] == "other").count() == 0

    def test_only_filters_charged_off(self, df):
        assert known_purpose(df).count() == 1
