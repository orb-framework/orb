from projex.lazymodule import lazy_import
from ..mysqlconnection import MySQLStatement

orb = lazy_import('orb')


class SCHEMA_INFO(MySQLStatement):
    def __call__(self, context):
        sql = u'SELECT t.table_name AS name,\n' \
              u'(\n' \
              u'   SELECT  group_concat(c.column_name)\n' \
              u'   FROM    information_schema.columns AS C\n' \
              u'   WHERE   c.table_schema = \'{namespace}\'\n' \
              u'   AND     c.table_name IN (t.table_name, CONCAT(t.table_name, \'_i18n\'))\n' \
              u') AS `fields`,\n' \
              u'(\n' \
              u'   SELECT  group_concat(i.index_name)\n' \
              u'   FROM    information_schema.statistics AS i\n' \
              u'   WHERE   i.table_schema = \'{namespace}\'\n' \
              u'   AND     i.table_name IN (t.table_name, CONCAT(t.table_name, \'_i18n\'))\n' \
              u') AS `indexes`\n' \
              u'FROM  information_schema.tables AS t\n' \
              u'WHERE t.table_schema = \'{namespace}\'\n' \
              u'AND   t.table_name NOT LIKE \'%%_i18n\''

        return sql.format(namespace=context.namespace or context.db.name()), {}


MySQLStatement.registerAddon('SCHEMA INFO', SCHEMA_INFO())
