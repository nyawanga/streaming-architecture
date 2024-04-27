## Architecture
 <code><img src="https://github.com/nyawanga/streaming-architecture/blob/main/docs/streaming.drawio.png?raw=true"/></code>

### Prerequisite
#### Python Environment
- create a python environment  using any tool of choice in my case I used virtualenvwrapper
- install the necessary libraries indicated in the root directory `requirements.txt` as shown below:
```bash
    pip install --no-cache -r requirements.txt
```

### [Docker](https://www.docker.com/)
- Install [dotnev](https://www.npmjs.com/package/dotenv) or [dotnevx](https://dotenvx.com/) to help load env variables when starting the service
- Have docker installed in your environment

### Spin Up the Service
- use this command to start the service
```bash
    dotenvx run -- docker-compose -f docker-compose.yml up
```
- check running services using the command below:
```bash
docker ps
```

#### [Postgres](https://www.postgresql.org/)
- To get postgres ready for streaming service you first need to change the config value for [write ahead log](https://www.postgresql.org/docs/current/wal-intro.html)
- By default it is commented out but it needs to be `wal = logical` otherwise debezium throws an error
- Make sure to restart the server after this
- Ensure there is a user who has right for `REPLICATION` best to create a user for the streaming service
- Create the relevant tables
- You can also use pgadmin to do this instead of using the terminal

#### [Clickhouse](https://clickhouse.com/)
- Create the relevant table that you want to replicate from postgres and ensure the table names are compartible
- Make the table engine to be `ENGINE = ReplaceMergeTree`.
- Ensure it is reachable.

#### [Kafka](https://kafka.apache.org)
- We use only one kafka broker and one zookeeper for this architecture.
- `kafdrop` provides us a graphical interface for our kafka instance and we can see the topics and messages streaming in from this portal.

#### [Debezium](https://debezium.io/)
- Due to it lacking clickhouse sink connectors we had to try use the custom created connector `clickhouse-kafka-connector`
- We need to mount the directory that contains the jar file to the debezium container under `/kafka/connector` directory.
- Choice of directory name is informed by the naming convention of other connectors already installed
N/B: Create this folder and mount it before hand.

#### [Superset](https://superset.apache.org)
- Superset needs to be bale to connect to clickhouse database hence we created a separate `Dockerfile.superset`
- In the file we indicate installation of relevant connectors used for database interactions
- We also have to run initial commands when it is started i.e 
    - Creation of the user to be able to login via the web interface
    - Database migration
    - Initialization
- Connecting to the clickhouse server from there should be direct with drivers being under `Other`

#### App
- This directory has relevant python files i.e 
    - Script used to generate the data needed for our architecture `pg_insert.py`
    - Script used to create clikchouse tables `create_clickhouse_tables.py`
    - Config file with sql statements for creating clickhouse tables `clickhouse_config.yml`
- To run these scripts you need to ensure you have activated the environment you created earlier.

### Docs
- This directory has folders hosting sql files and anyb relevant files used in the code
    #### postges
    - This will host the postgres schema and any relevant sql queries run on the server.

    #### clickhouse 
    - This will hold the server schema and relevant sql queries run in it.

    #### superset
    - This will video demo of apache superset and dashboard screenshots.

#### Credentials
- For purposes of safety and best practice there is no hardcoding of credentials.
- We make use of `.env` file to carry all our creds.

### Notes:
- Currently the whole architecture comes up nicely and the docker containers can communicate with each other.
- Could not be able to get the clockhouse sink to work had to write queries on postgres database
- Find a working solution for the [clickhouse sink connectors](https://github.com/ClickHouse/clickhouse-kafka-connect)

### Update
- Managed to get the kafka to clickhouse working but using [confluentinc/kafka-connect-jdbc](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc/) connector.
- Also videos from this guy [Robbin Moffat](https://www.youtube.com/watch?v=vI_L9irU9Pc&t=950s) helped alot in setting the sink connector.
- See my dockerfile for creating the container `Dockerfile.connect` on how I go about creating the container image and the jdbc connector.

### Gotchas:

#### Postgres:
- `Postgres server wal_level property must be 'logical' but is: 'replica'`
