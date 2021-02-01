import os

SERVER = os.environ["SERVER"]
PORT = os.environ["PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
TABLE_NAME = os.environ["TABLE_NAME"]


def connect(target):
    return (
        target.format("jdbc")
        .option("url", f"jdbc:postgresql://{SERVER}:{PORT}/{DB_NAME}")
        .option("dbtable", TABLE_NAME)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
    )
