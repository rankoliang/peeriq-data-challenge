import pytest
from pyspark.sql import SparkSession
from src.transform_data import goodStanding


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Transforms").master("local").getOrCreate()
    yield spark
    spark.stop()


class TestTransforms:
    def test_good_standing(self, spark):
        data = [(0, "Charged Off"), (1, "Fully Paid")]
        df = spark.createDataFrame(data, ["id", "loan_status"])

        assert goodStanding(df).filter(df["loan_status"] == "Charged Off").count() == 0
