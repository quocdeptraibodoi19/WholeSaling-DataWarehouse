#!/bin/bash

# Set locale environment variables to avoid locale warnings
export LANGUAGE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "PostgreSQL is not installed. Installing now..."
    # Update package lists
    sudo apt-get update
    # Install PostgreSQL
    sudo apt-get install -y postgresql postgresql-contrib
else
    echo "PostgreSQL is already installed."
fi

# Getting the credentials of the db from the local env
source ../../local-env.sh

export PGPASSWORD=$visualization_opdb_password

# Connect to the remote PostgreSQL database and create the database if it doesn't exist
echo "Connecting to the remote PostgreSQL database..."
psql -h $visualization_opdb_host -p $visualization_opdb_port -U $visualization_opdb_user -d postgres -t -c "CREATE DATABASE visualization"

# Switch to the target database
echo "Switching to the target database..."
psql -h $visualization_opdb_host -p $visualization_opdb_port -U $visualization_opdb_user -d $visualization_opdb_dbname -t -c "\c $visualization_opdb_dbname"

# Create the chart table
echo "Creating the chart table..."
psql -h $visualization_opdb_host -p $visualization_opdb_port -U $visualization_opdb_user -d $visualization_opdb_dbname -t -c "
CREATE EXTENSION \"uuid-ossp\";

CREATE TABLE chart (
    chart_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chart_name VARCHAR(255),
    state JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_chart_id ON chart USING btree(chart_id);
"

# Unset environments related to Postgres
unset PGPASSWORD
unset LANGUAGE
unset LC_ALL
unset LANG
