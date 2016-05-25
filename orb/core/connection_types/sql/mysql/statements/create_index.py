from projex.lazymodule import lazy_import
from ..mysqlconnection import MySQLStatement

orb = lazy_import('orb')


class CREATE_INDEX(MySQLStatement):
    def __call__(self, index, checkFirst=False):
        """
        Modifies the table to add and remove the given columns.

        :param model: <orb.Model>
        :param add: [<orb.Column>, ..]
        :param remove: [<orb.Column>, ..]

        :return: <bool>
        """
        schema_name = index.schema().dbname()
        index_name = index.dbname()
        cmd = 'CREATE' if not index.testFlag(index.Flags.Unique) else 'CREATE UNIQUE'

        cols = ['`{0}`'.format(col.field())
                if isinstance(col, orb.AbstractStringColumn) and not col.testFlag(col.Flags.CaseSensitive)
                else '`{0}`'.format(col.field())
                for col in index.columns()]

        cmd = '{0} INDEX `{1}` ON `{2}` ({3})'.format(cmd, index_name, schema_name, ', '.join(cols))

        if checkFirst:
            cmd = """\
            DO $$
            BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = '{0}'
                AND indexname = '{1}'
            ) THEN {2};
            END IF;
            END$$;
            """.format(index.schema().namespace() or orb.Context().db.name(), index_name, cmd)
        else:
            cmd += ';'

        return cmd, {}


MySQLStatement.registerAddon('CREATE INDEX', CREATE_INDEX())
