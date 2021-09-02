CREATE TABLE eth_last_block
(
    chain_id     INTEGER PRIMARY KEY,
    block_number INTEGER NOT NULL
);

CREATE TABLE eth_events
(
    chain_id           INTEGER NOT NULL,
    event_transaction  BLOB    NOT NULL,
    event_index        INTEGER NOT NULL,
    event_data         BLOB    NOT NULL,
    event_block_number INTEGER NOT NULL,
    event_block        BLOB    NOT NULL,
    address            BLOB    NOT NULL,
    target_event_block INTEGER NOT NULL,
    status             INTEGER NOT NULL,
    PRIMARY KEY (chain_id, event_transaction)
);
