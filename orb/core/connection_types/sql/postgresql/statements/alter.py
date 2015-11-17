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
            if col.testFlag(col.Flags.Translatable):
                add_i18n.append(col)
            else:
                add_standard.append(col)

        # add standard columns
        command = []
        if add_standard:
            command.append(u'ALTER {0} "{1}"'.format(typ, model.schema().dbname()))
            command.append(u'\t' + ',\n\t'.join([ADD_COLUMN(col) for col in add_standard]))
            command.append(u';')

        # add i18n columns
        if add_i18n:
            template = u"""\
            CREATE TABLE IF NOT EXISTS "{table}_i18n" (
              "locale" CHARACTER VARYING(5)
              "{table}_id" BIGINT REFERENCES "{table}" ("id") ON DELETE CASCADE,
              CONSTRAINT "{table}_i18n_pkey PRIMARY KEY ("locale", "{table}_id")
            ) WITH (OIDS=FALSE);
            ALTER TABLE "{table}_i18n" OWNER TO "{owner}";
            """
            command.append(template.format(table=model.schema().dbname(), owner=owner))
            command.append(u'ALTER TABLE "{0}_i18n"'.format(model.schema().dbname()))
            command.append(u'\t' + u',\n\t'.join([ADD_COLUMN(col) for col in add_i18n]))
            command.append(u';')

        return '\n'.join(command)


PSQLStatement.registerAddon('ALTER', ALTER())
