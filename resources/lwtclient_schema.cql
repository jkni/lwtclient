CREATE KEYSPACE IF NOT EXISTS lwtclient WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS lwtclient.registers
(
    id int PRIMARY KEY,
    contents int
);
