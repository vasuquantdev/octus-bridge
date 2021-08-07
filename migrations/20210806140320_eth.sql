-- Add migration script here

CREATE TABLE ETH_LAST_BLOCK
(
    chain_id          INTEGER PRIMARY KEY,
    block_number int8 NOT NULL
);


-- CREATE TABLE ETH_DATA
-- (
--
-- )