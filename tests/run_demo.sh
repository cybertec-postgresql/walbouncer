#!/bin/bash

msg()
{
    echo -e "\e[1m\e[32m *" "$@"
    echo -n -e "\e[0m\e[39m"
}

shutdown_pg()
{
   if [ -e "$1/postmaster.pid" ]; then
        pg_ctl -D "$1" -m fast stop
   fi
}

cleanup()
{
    msg "Doing cleanup from previous runs"
    killall -q walbouncer
    
    shutdown_pg "$MASTER_DATA"
    rm -rf "$MASTER_DATA"
    shutdown_pg "$SLAVE1_DATA"
    rm -rf "$SLAVE1_DATA"
    shutdown_pg "$SLAVE2_DATA"
    rm -rf "$SLAVE2_DATA"
    
    rm walbouncer.log
    rm -rf tablespaces
    for db in master slave1 slave2; do
        for spc in slave1 slave2; do
            mkdir -p tablespaces/$db/$spc
        done
    done
}

setup_master()
{
    msg "Setting up master server"
    initdb -k --auth=trust -U postgres -N -D "$MASTER_DATA" 2>&1 > initdb.log || exit
    rm initdb.log
    (cat <<EOF
        port=5432
        max_connections=50
        shared_buffers=32MB
        wal_level = hot_standby
        max_wal_senders = 5
        wal_keep_segments = 50
        hot_standby = on
        logging_collector = on
        log_directory = 'pg_log'
        log_filename = 'postgresql.log'
        log_connections = on
        log_disconnections = on
        log_line_prefix = '[%m] %u %d '
        # log_min_messages = debug2
	hot_standby_feedback = on
EOF
    ) > "$MASTER_DATA/postgresql.conf"
    (cat <<EOF
        local   all             all                                     trust
        host    all             all             127.0.0.1/32            trust
        local   replication     all                                     trust
        host    replication     all             127.0.0.1/32            trust
EOF
    ) > "$MASTER_DATA/pg_hba.conf"
    pg_ctl -D "$MASTER_DATA" -l "$MASTER_DATA/startup.log" -w start || exit
    msg "Master started"
}

master_sql()
{
    psql -h localhost -p 5432 -U postgres -c "$1"
}

slave1_sql()
{
    psql -h localhost -p 5434 -U postgres -c "$1"
}

setup_master_tablespaces()
{
    msg "Setting up tablespace spc_slave1 and spc_slave2 on master"
    master_sql "CREATE TABLESPACE spc_slave1 LOCATION '$WD/tablespaces/master/slave1'" || exit
    master_sql "CREATE TABLESPACE spc_slave2 LOCATION '$WD/tablespaces/master/slave2'" || exit
}

setup_slave()
{
    SLAVE_NAME=$1
    msg "Taking a backup for slave $SLAVE_NAME"
    pg_basebackup -D "$WD/$SLAVE_NAME" -h localhost -p 5432 -U postgres \
        --checkpoint=fast \
        --tablespace-mapping="$WD/tablespaces/master/slave1=$WD/tablespaces/$SLAVE_NAME/slave1" \
        --tablespace-mapping="$WD/tablespaces/master/slave2=$WD/tablespaces/$SLAVE_NAME/slave2"
    (cat <<EOF
        recovery_target_timeline = 'latest'
        standby_mode = on
        primary_conninfo = 'host=localhost port=5433 user=postgres application_name=spc_$SLAVE_NAME'
EOF
    ) > "$WD/$SLAVE_NAME/recovery.conf"
    rm "$WD/$SLAVE_NAME/pg_log/postgresql.log"
    sed -i "s/port=5432/port=$2/" "$WD/$SLAVE_NAME/postgresql.conf"
}

start_walbouncer()
{
    msg "Starting walbouncer on port 5433 (output in walbouncer.log)"
    nohup $WALBOUNCER -p 5433 -vv > walbouncer.log &
}

start_slave()
{
    SLAVE_NAME=$1
    msg "Starting slave $SLAVE_NAME"
    pg_ctl -D "$WD/$SLAVE_NAME" -l "$WD/$SLAVE_NAME/startup.log" -w start || exit
}

create_test_tables()
{
    msg "Creating test tables on_all, on_slave1 and on_slave2"
    psql -h localhost -p 5432 -U postgres -v ON_ERROR_STOP=1 -f - <<SQL
CREATE TABLE on_all (id serial primary key,
                     v int,
                     filler text,
                     tstamp timestamptz default current_timestamp);
CREATE TABLE on_slave1 (id serial primary key,
                        v int,
                        filler text,
                        tstamp timestamptz default current_timestamp) TABLESPACE spc_slave1;
CREATE TABLE on_slave2 (id serial primary key,
                        v int, filler text,
                        tstamp timestamptz default current_timestamp) TABLESPACE spc_slave2;
SQL
}

check_replication()
{
    master_sql "INSERT INTO on_all (v) VALUES (1)"
    master_sql "INSERT INTO on_slave1 (v) VALUES (1)"
    master_sql "INSERT INTO on_slave2 (v) VALUES (1)"
    
    sleep 1
    
    msg "Data in the default tablespace"
    slave1_sql "SELECT * FROM on_all"
    msg "Data in the slave 1 tablespace"
    slave1_sql "SELECT * FROM on_slave1"
    msg "Data in the slave 2 tablespace (expect error)"
    slave1_sql "SELECT * FROM on_slave2"
    
    msg "Generating some data in spc_slave2"
    master_sql "INSERT INTO on_slave2 (v, filler) SELECT x, REPEAT(' ', 1000) FROM generate_series(1,10000) x"
    
    msg "Waiting 3s for replication to complete"
    sleep 3
    msg "Master disk usage from spc_slave2:"
    du -sh $WD/tablespaces/master/slave2
    msg "Slave 1 disk usage from spc_slave2:"
    du -sh $WD/tablespaces/slave1/slave2
}

TEST_HOME="$(dirname $0)"
cd "$TEST_HOME"
WD="$(pwd)"

MASTER_DATA="$WD/master"
SLAVE1_DATA="$WD/slave1"
SLAVE2_DATA="$WD/slave2"
WALBOUNCER="$WD/../src/walbouncer"

cleanup
setup_master
setup_master_tablespaces
setup_slave slave1 5434
# Will enable this when walbouncer supports forking
#setup_slave slave2 5435
start_walbouncer
start_slave slave1

create_test_tables

check_replication

msg "Master is running on port 5432, slave1 on 5434."
msg "walbouncer.log contains heaps of debug info."
msg "Have fun!"
