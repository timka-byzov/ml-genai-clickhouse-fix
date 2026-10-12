SELECT
    state,
    winner.1 AS category,
    winner.2 AS max_amount,
    winner.3 AS row_id
FROM
(
    SELECT state, argMaxMerge(winner) AS winner
    FROM homework.max_by_state
    GROUP BY state
)
ORDER BY state;
