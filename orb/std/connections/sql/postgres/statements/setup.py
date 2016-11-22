from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class SETUP(PSQLStatement):
    def __call__(self, db):
        sql = [
            u'CREATE EXTENSION IF NOT EXISTS hstore;',
            u'DROP AGGREGATE IF EXISTS hstore_agg(hstore);',
            u'CREATE AGGREGATE hstore_agg(hstore) (',
            u'   sfunc=hs_concat,',
            u'   stype=hstore',
            u');',
            u'CREATE OR REPLACE FUNCTION array_sort(ANYARRAY)',
            u'RETURNS ANYARRAY LANGUAGE SQL',
            u'AS $$',
            u'SELECT ARRAY(',
            u'   SELECT $1[s.i] AS "foo"',
            u'   FROM',
            u'       generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)',
            u'   ORDER BY foo',
            u');',
            u'$$;'
        ]
        return u'\n'.join(sql), {}


PSQLStatement.registerAddon('SETUP', SETUP())
