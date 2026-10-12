SELECT
    state,
    winner.1 AS category,
    winner.2 AS max_amount,
    winner.3 AS row_id
FROM
(
    SELECT
        state,
        argMax(tuple(category, amount, row_id), tuple(amount, row_id)) AS winner
    FROM homework.transactions
    GROUP BY state
)
ORDER BY state;
