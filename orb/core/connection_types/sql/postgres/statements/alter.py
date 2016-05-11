from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class ALTER(PSQLStatement):
    def __call__(self, model, add=None, remove=None, owner='postgres'):
        """
        Modifies the table to add and remove the given columns.

        :param model: <orb.Model>
        :param add: [<orb.Column>, ..]
        :param remove: [<orb.Column>, ..]

        :return: <bool>
        """
        data = {}
        ADD_COLUMN = self.byName('ADD COLUMN')

        # determine what kind of model we're modifying
        if issubclass(model, orb.Table):
            typ = 'TABLE'
        else:
            raise orb.errors.OrbError('Cannot alter {0}'.format(type(model)))

        # determine the i18n and standard columns
        add_i18n = []
        add_standard = []
        for col in add or []:
            # virtual columns do not exist in the database
            if col.testFlag(col.Flags.Virtual):
                continue

            if col.testFlag(col.Flags.I18n):
                add_i18n.append(col)
            else:
                add_standard.append(col)

        # add standard columns
        if add_standard:
            field_statements = []

            for col in add_standard:
                field_statement, field_data = ADD_COLUMN(col)
                data.update(field_data)
                field_statements.append(field_statement)

            sql_options = {
                'type': typ,
                'namespace': model.schema().namespace() or 'public',
                'name': model.schema().dbname(),
                'fields': u'\t' + ',\n\t'.join(field_statements)
            }
            sql = (
                u'ALTER {type} "{namespace}"."{name}"\n'
                u'{fields};'
            ).format(**sql_options)
        else:
            sql = ''

        # add i18n columns
        if add_i18n:
            id_column = model.schema().idColumn()
            id_type = id_column.dbType('Postgres')

            field_statements = []

            for col in add_i18n:
                field_statement, field_data = ADD_COLUMN(col)
                data.update(field_data)
                field_statements.append(field_statement)

            i18n_options = {
                'namespace': model.schema().namespace() or 'public',
                'table': model.schema().dbname(),
                'fields': u'\t' + ',\n\t'.join(field_statements),
                'owner': owner,
                'id_type': id_type,
                'id_field': id_column.field()
            }

            i18n_sql = (
                u'CREATE TABLE IF NOT EXISTS "{namespace}"."{table}_i18n" (\n'
                u'  "locale" CHARACTER VARYING(5),\n'
                u'  "{table}_id" {id_type} REFERENCES "{namespace}"."{table}" ("{id_field}") ON DELETE CASCADE,\n'
                u'  CONSTRAINT "{table}_i18n_pkey" PRIMARY KEY ("locale", "{table}_id")\n'
                u') WITH (OIDS=FALSE);'
                u'ALTER TABLE "{namespace}"."{table}_i18n" OWNER TO "{owner}";'
                u'ALTER TABLE "{namespace}"."{table}_i18n"'
                u'{fields};'
            ).format(**i18n_options)

            sql += '\n' + i18n_sql

        return sql, data


PSQLStatement.registerAddon('ALTER', ALTER())
