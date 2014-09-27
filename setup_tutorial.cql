drop keyspace if exists tutorial;

create KEYSPACE  tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use tutorial;

create table sensor ( 
    sensor_id uuid primary key,
    date_installed timestamp
);

create table sensor_data (
    sensor_id uuid,
    date_created timeuuid,
    reading int,
    primary key (sensor_id, date_created)
);

