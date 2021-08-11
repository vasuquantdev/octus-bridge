-- Add migration script here

CREATE TABLE ETH_LAST_BLOCK
(
    chain_id     INTEGER PRIMARY KEY,
    block_number INTEGER NOT NULL
);

CREATE TABLE eth_events
(
    entry_id   TEXT PRIMARY KEY NOT NULL,
    event_data BLOB NOT NULL
)