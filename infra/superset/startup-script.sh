#!/usr/bin/env bash
# echo "Waiting for PostgreSQL to start..."
# while ! nc -z superset_postgres 5432; do   
#   sleep 0.1
# done
echo "PostgreSQL started"

superset fab create-admin \
--username ${SUPERSET_USERNAME} \
--firstname ${SUPERSET_FIRST_NAME} \
--lastname ${SUPERSET_LAST_NAME} \
--email ${SUPERSET_EMAIL} \
--password ${SUPERSET_PASSWORD} 

# Perform database migrations
superset db upgrade

# Initialize Superset (create admin user, etc.)
superset init

# Start the Superset application (if not using the CMD from the Dockerfile)
#superset run -p 8088 --with-threads --reload --debugger

# Starting server
/bin/sh -c /usr/bin/run-server.sh
