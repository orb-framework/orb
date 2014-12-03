-- ensure hstore is installed
CREATE EXTENSION IF NOT EXISTS hstore;

-- define the hstore_agg aggregate
DROP AGGREGATE IF EXISTS hstore_agg(hstore);
CREATE AGGREGATE hstore_agg(hstore) (
    sfunc=hs_concat,
    stype=hstore
);