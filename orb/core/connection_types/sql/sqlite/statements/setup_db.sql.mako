-- ensure hstore is installed
CREATE EXTENSION IF NOT EXISTS hstore;

-- define the hstore_agg aggregate
DROP AGGREGATE IF EXISTS hstore_agg(hstore);
CREATE AGGREGATE hstore_agg(hstore) (
    sfunc=hs_concat,
    stype=hstore
);

-- define the array_sort method
CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(
    SELECT $1[s.i] AS "foo"
    FROM
        generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)
    ORDER BY foo
);
$$;