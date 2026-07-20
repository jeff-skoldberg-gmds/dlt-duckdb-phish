SELECT
    '1.0'::text AS era,
    '1883-01-01'::date AS start_date,
    '2000-12-31'::date AS end_date
UNION
SELECT
    '2.0',
    '2002-12-31',
    '2004-12-31'
UNION
SELECT
    '3.0',
    '2009-01-01',
    NULL
