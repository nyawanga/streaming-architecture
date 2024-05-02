#!/bin/bash

# Path to PostgreSQL configuration file
CONFIG_FILE="/var/lib/postgresql/data/postgresql.conf"

# Modify PostgreSQL configuration
sed -i "s/^#wal_level.*/wal_level = logical/" $CONFIG_FILE
sed -i "s/^#max_replication_slots.*/max_replication_slots = 4/" $CONFIG_FILE
sed -i "s/^#max_wal_senders.*/max_wal_senders = 4/" $CONFIG_FILE

# Restart PostgreSQL service (handled by Docker)
echo "PostgreSQL configuration has been updated."

# Notify the need for a restart or handle other initialization tasks
