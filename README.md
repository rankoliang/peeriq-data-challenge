# PeerIQ Data Challenge

## Getting Started

These instructions will assume you are using a unix based OS and have python
installed on your system. The python version used is 3.7.9.
If this is your first time running this, create a virutal environment using

```
python3 -m venv venv
```

Then, activate your virtual environment by running

```
source venv/bin/activate
```

Finally, install the python dependencies using

```
pip3 install -r requirements.txt
```

The following commands will assume you are
in your virtual environment if you are running locally.

## Running in AWS

#### Assumptions:

You have a postgres database in AWS or your preferred provider
with all of the required credentials and security permissions set.
You can configure this by clicking on the `VPC Security Groups`
link on your database page and editing the `Inbound rules`.
Allow any ips of the environments you are running.

#### Steps

Create an s3 bucket and upload the csv files into it.
You will also need to upload the `postgresql-42.2.18.jar` file
located in `jars/` and the `transform_data.py` file located
in `dist/`.

Create an EMR cluster. Select step execution as the launch mode.
For the spark-submit options, use:

```
--jars s3://<bucket-name>/postgresql-42.2.18.jar
```

The application location will be

```
s3://<bucket-name>/transform_data.py
```

Configure the arguments. An Example can be found below:

```
--data-source s3://<bucket-name>/*.csv --server lorem.12345.location.rds.amazonaws.com --port 5432 --db-name peeriq --db-user postgres --db-password password --table-name loans
```

Create your cluster and wait for the task to finish running. You can use psql to verify
that the results have been correctly loaded.

## Arguments

For the file `build/transform_data.py`

- `data-source` - path of the data files, such as `s3://<bucket-name>/*.csv`
- `server` - server where postgresql is running (RDS hostname)
- `port` - port used to connect to the postgresql server
- `db-name` - name of the database where the data will be loaded to
- `db-user` - username used to access the server
- `db-password` - password associated with the username
- `table-name` - name of the table where data will be loaded to

## Running the file locally

Run the file with

```
spark-submit --jars jars/postgresql-42.2.18.jar \
    dist/transform_data.py \
    --data-source data/*.csv \
    --server localhost \
    --port 5432 \
    --db-name <db-name> \
    --db-user <db-user> \
    --db-password <db-password> \
    --table-name loans
```

### Tests

To run the test suite, run

```
python3 -m pytest
```
