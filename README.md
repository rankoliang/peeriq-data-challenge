# PeerIQ Data Challenge

## Getting Started

These instructions will assume you are using a unix based OS and have python
installed on your system.
If this is your first time running this, create a virutal environment using

```
python3 -m venv venv
```

Then, activate your virutal environment by running

```
source venv/bin/activate
```

Finally, install the python dependencies using

```
pip3 install -r requirements.txt
```

### Setting up your environment variables

Copy the `.sample_env` file to `.env` and replace dummy values with your own.

- `SERVER` - server where postgresql is running (RDS hostname)
- `PORT` - port used to connect to the postgresql server
- `DB_NAME` - name of the database where the data will be loaded to
- `DB_USER` - username used to access the server
- `DB_PASSWORD` - password associated with the username
- `TABLE_NAME` - name of the table where data will be loaded to

### Running Locally

To load the database locally with the sample data, run

```
env $(cat .env | xargs) python3 -m src.transform_data
```

from the project's root directory.

### Tests

To run the test suite, run

```
python3 -m pytest
```
