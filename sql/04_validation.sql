SELECT 'baseline' AS variant, count() AS rows, uniqExact(row_id) AS unique_rows
FROM homework.transactions
UNION ALL
SELECT 'optimized', count(), uniqExact(row_id)
FROM homework.transactions_optimized;

SELECT
    table,
    sum(rows) AS rows,
    sum(data_compressed_bytes) AS compressed_bytes,
    sum(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE database = 'homework' AND active
GROUP BY table
ORDER BY table;
