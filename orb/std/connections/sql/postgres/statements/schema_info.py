from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class SCHEMA_INFO(PSQLStatement):
    def __call__(self, context):
        sql = u'SELECT t.table_name AS name,\n' \
              u'(\n' \
              u'   SELECT  array_agg(c.column_name::varchar)\n' \
              u'   FROM    information_schema.columns AS C\n' \
              u'   WHERE   c.table_schema = \'{namespace}\'\n' \
              u'   AND     c.table_name IN (t.table_name, t.table_name || \'_i18n\')\n' \
              u') AS "fields",\n' \
              u'(\n' \
              u'   SELECT  array_agg(i.indexname)\n' \
              u'   FROM    pg_indexes AS i\n' \
              u'   WHERE   i.schemaname = \'{namespace}\'\n' \
              u'   AND     i.tablename IN (t.table_name, t.table_name || \'_i18n\')\n' \
              u') AS "indexes"\n' \
              u'FROM  information_schema.tables AS t\n' \
              u'WHERE t.table_schema = \'{namespace}\'\n' \
              u'AND   t.table_name NOT ILIKE \'%%_i18n\''

        return sql.format(namespace=context.namespace or 'public'), {}


PSQLStatement.registerAddon('SCHEMA INFO', SCHEMA_INFO())
