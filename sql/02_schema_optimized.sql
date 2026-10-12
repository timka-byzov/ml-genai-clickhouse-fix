CREATE TABLE IF NOT EXISTS homework.transactions_optimized
(
    row_id UInt64 CODEC(Delta, ZSTD(1)),
    transaction_time DateTime('UTC') CODEC(Delta, ZSTD(1)),
    state LowCardinality(String),
    category LowCardinality(String),
    amount Decimal(18, 2) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (state, row_id);

CREATE TABLE IF NOT EXISTS homework.max_by_state
(
    state LowCardinality(String),
    winner AggregateFunction(
        argMax,
        Tuple(String, Decimal(18, 2), UInt64),
        Tuple(Decimal(18, 2), UInt64)
    )
)
ENGINE = AggregatingMergeTree
ORDER BY state;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.max_by_state_mv
TO homework.max_by_state
AS SELECT
    state,
    argMaxState(tuple(toString(category), amount, row_id), tuple(amount, row_id)) AS winner
FROM homework.transactions_optimized
GROUP BY state;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.transactions_optimized_mv
TO homework.transactions_optimized
AS SELECT row_id, transaction_time, state, category, amount
FROM homework.transactions;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.transactions_mv
TO homework.transactions
AS SELECT row_id, transaction_time, state, category, amount
FROM homework.transactions_kafka;
