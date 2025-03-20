#!/bin/bash

set -eu

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
    killall -q walbouncer || :

    shutdown_pg "$MASTER_DATA"
    rm -rf "$MASTER_DATA"
    shutdown_pg "$STANDBY1_DATA"
    rm -rf "$STANDBY1_DATA"
    shutdown_pg "$STANDBY2_DATA"
    rm -rf "$STANDBY2_DATA"

    rm -f walbouncer.log
    rm -rf $TABLESPACES
}

prepare_tablespaces()
{
    for db in master standby1 standby2; do
        for spc in standby1 standby2; do
            mkdir -p $TABLESPACES/$db/$spc
        done
    done
}

setup_master()
{
    msg "Setting up master server"
    initdb -k --auth=trust -U postgres -N -D "$MASTER_DATA" 2>&1 > initdb.log || exit
    rm initdb.log
    (cat <<EOF
        port=$MASTER_PORT
        max_connections = 50
        shared_buffers = 32MB
        wal_level = hot_standby
        max_wal_senders = 5
        wal_keep_size = 1GB
        hot_standby = on
        logging_collector = on
        log_directory = 'log'
        log_filename = 'postgresql.log'
        log_connections = on
        log_disconnections = on
        log_line_prefix = '[%m] %u %d '
        # log_min_messages = debug2
        hot_standby_feedback = on
        fsync = off
        unix_socket_directories='/tmp'
EOF
    ) > "$MASTER_DATA/postgresql.conf"
    (cat <<EOF
        local   all             all                                     trust
        host    all             all             127.0.0.1/32            trust
        host    all             all             ::1/128                 trust
        local   replication     all                                     trust
        host    replication     all             127.0.0.1/32            trust
        host    replication     all             ::1/128                 trust
EOF
    ) > "$MASTER_DATA/pg_hba.conf"
    pg_ctl -D "$MASTER_DATA" -l "$MASTER_DATA/startup.log" -w start || exit
    msg "Master started"
}

master_sql()
{
    psql -h localhost -p $MASTER_PORT -U postgres -c "$1"
}

standby1_sql()
{
    psql -h localhost -p $STANDBY1_PORT -U postgres -c "$1"
}

setup_master_tablespaces()
{
    msg "Setting up tablespace spc_standby1 and spc_standby2 on master"
    master_sql "CREATE TABLESPACE spc_standby1 LOCATION '$TABLESPACES/master/standby1'" || exit
    master_sql "CREATE TABLESPACE spc_standby2 LOCATION '$TABLESPACES/master/standby2'" || exit
}

setup_standby()
{
    STANDBY_NAME=$1
    msg "Taking a backup for standby $STANDBY_NAME"
    pg_basebackup -D "$WD/$STANDBY_NAME" -h localhost -p $MASTER_PORT -U postgres \
        --checkpoint=fast \
        --tablespace-mapping="$TABLESPACES/master/standby1=$TABLESPACES/$STANDBY_NAME/standby1" \
        --tablespace-mapping="$TABLESPACES/master/standby2=$TABLESPACES/$STANDBY_NAME/standby2"
    (cat <<EOF
        recovery_target_timeline = 'latest'
        primary_conninfo = 'host=localhost port=$WALBOUNCER_PORT user=postgres application_name=$STANDBY_NAME'
EOF
    ) > "$WD/$STANDBY_NAME/postgresql.auto.conf"
    touch "$WD/$STANDBY_NAME/standby.signal"
    rm "$WD/$STANDBY_NAME/log/postgresql.log"
    sed -i "s/port=$MASTER_PORT/port=$2/" "$WD/$STANDBY_NAME/postgresql.conf"
}

start_walbouncer()
{
    msg "Starting walbouncer on port $WALBOUNCER_PORT (output in walbouncer.log)"
   # nohup
    $WALBOUNCER -c democonf.yaml -v &
    sleep 1
}

start_standby()
{
    STANDBY_NAME=$1
    msg "Starting standby $STANDBY_NAME"
    pg_ctl -D "$WD/$STANDBY_NAME" -l "$WD/$STANDBY_NAME/startup.log" -w start || exit
}

create_test_tables()
{
    msg "Creating test tables on_all, on_standby1 and on_standby2"
    psql -h localhost -p $MASTER_PORT -U postgres -v ON_ERROR_STOP=1 -f - <<SQL
CREATE TABLE on_all (id serial primary key,
                     v int,
                     filler text,
                     tstamp timestamptz default current_timestamp);
CREATE TABLE on_standby1 (id serial primary key,
                        v int,
                        filler text,
                        tstamp timestamptz default current_timestamp) TABLESPACE spc_standby1;
CREATE TABLE on_standby2 (id serial primary key,
                        v int, filler text,
                        tstamp timestamptz default current_timestamp) TABLESPACE spc_standby2;
SQL
}

check_replication()
{
    master_sql "INSERT INTO on_all (v) VALUES (1)"
    master_sql "INSERT INTO on_standby1 (v) VALUES (1)"
    master_sql "INSERT INTO on_standby2 (v) VALUES (1)"

    sleep 1

    msg "Data in the default tablespace"
    standby1_sql "SELECT * FROM on_all"
    msg "Data in the standby 1 tablespace"
    standby1_sql "SELECT * FROM on_standby1"
    msg "Data in the standby 2 tablespace (expect error)"
    standby1_sql "SELECT * FROM on_standby2" || :

    msg "Generating some data in spc_standby2"
    master_sql "INSERT INTO on_standby2 (v, filler) SELECT x, REPEAT(' ', 1000) FROM generate_series(1,10000) x"

    msg "Waiting 3s for replication to complete"
    sleep 3
    msg "Master disk usage from spc_standby2:"
    du -sh $TABLESPACES/master/standby2
    msg "Standby 1 disk usage from spc_standby2:"
    du -sh $TABLESPACES/standby1/standby2
}

check_different_rmgrs()
{
    msg "Creating test data for different rmgr-s in rmgr_test1"
	master_sql "create table rmgr_test1 (c1 int, c2 text)"
	master_sql "create index on rmgr_test1 using brin (c1)"
	master_sql "create index on rmgr_test1 using gin (to_tsvector('english', c2));"
	master_sql "create extension btree_gin"
	master_sql "create index on rmgr_test1 using gin (c1)"
	master_sql "create extension btree_gist"
	master_sql "create index on rmgr_test1 using gist (c1)"
	master_sql "insert into rmgr_test1 select 1, 'hello'"
	sleep 1
	msg "Standby 1 data in rmgr_test1"
	standby1_sql "SELECT * FROM rmgr_test1"
	standby1_sql "\d rmgr_test1"
}

increment_master_timeline()
{
    msg "Incrementing master timeline"
    pg_ctl -D "$MASTER_DATA" -l "$MASTER_DATA/startup.log" -w stop || exit
    (cat <<EOF
        recovery_target_timeline = 'latest'
        restore_command = 'false'
EOF
    ) > "$MASTER_DATA/postgresql.auto.conf"
    touch "$WD/$STANDBY_NAME/standby.signal"
    pg_ctl -D "$MASTER_DATA" -l "$MASTER_DATA/startup.log" -w start || exit
}

TEST_HOME="$(dirname $0)"
cd "$TEST_HOME"
WD="$(pwd)"

MASTER_DATA="$WD/master"
MASTER_PORT="6432"
STANDBY1_DATA="$WD/standby1"
STANDBY1_PORT="6434"
STANDBY2_DATA="$WD/standby2"
: ${WALBOUNCER:="$WD/../src/walbouncer"}
WALBOUNCER_PORT=6433
TABLESPACES=/tmp/tablespaces

cleanup
trap "killall postgres walbouncer" ERR

prepare_tablespaces
setup_master
setup_master_tablespaces
setup_standby standby1 $STANDBY1_PORT
# Will enable this when walbouncer supports forking
#setup_standby standby2 5435
increment_master_timeline
start_walbouncer
start_standby standby1

create_test_tables

check_replication

#check_different_rmgrs

#msg "Master is running on port $MASTER_PORT, standby1 on $STANDBY1_PORT."
#msg "walbouncer.log contains heaps of debug info."
#msg "Have fun!"

cleanup
