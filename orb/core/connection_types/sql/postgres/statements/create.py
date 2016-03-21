from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class CREATE(PSQLStatement):
    def __call__(self, model, owner='', includeReferences=True):
        if issubclass(model, orb.Table):
            return self._createTable(model, owner, includeReferences)
        elif issubclass(model, orb.View):
            return self._createView(model, owner, includeReferences)
        else:
            raise orb.errors.OrbError('Cannot create model for type: '.format(type(model)))

    def _createTable(self, model, owner, includeReferences):
        ADD_COLUMN = self.byName('ADD COLUMN')

        add_i18n = []
        add_standard = []

        # divide columns between standard and translatable
        for col in model.schema().columns(recurse=False).values():
            if not includeReferences and isinstance(col, orb.ReferenceColumn):
                continue

            # virtual flags do not exist in the database
            elif col.testFlag(col.Flags.Virtual):
                continue

            if col.testFlag(col.Flags.I18n):
                add_i18n.append(col)
            else:
                add_standard.append(col)

        # create the standard model
        cmd_body = []
        if add_standard:
            cmd_body += [ADD_COLUMN(col)[0].replace('ADD COLUMN ', '') for col in add_standard]

        # get the primary column
        pcol = ''
        id_column = model.schema().idColumn()
        if id_column:
            pcol = '"{0}"'.format(id_column.field())
            cmd_body.append('CONSTRAINT "{0}_pkey" PRIMARY KEY ({1})'.format(model.schema().dbname(), pcol))

        body = ',\n\t'.join(cmd_body)
        if body:
            body = '\n\t' + body + '\n'

        inherits = model.schema().inherits()
        if inherits:
            inherits_model = orb.system.model(inherits)
            if not inherits_model:
                raise orb.errors.ModelNotFound(inherits)

            inherits = '\nINHERITS ("{0}")\n'.format(inherits_model.schema().dbname())

        cmd  = 'CREATE TABLE IF NOT EXISTS "{table}" ({body}) {inherits}WITH (OIDS=FALSE);\n'
        if owner:
            cmd += 'ALTER TABLE "{table}" OWNER TO "{owner}";'
        cmd = cmd.format(table=model.schema().dbname(), body=body, inherits=inherits, owner=owner)

        # create the i18n model
        if add_i18n:
            id_column = model.schema().idColumn()
            id_type = id_column.dbType('Postgres')

            i18n_body = ',\n\t'.join([ADD_COLUMN(col)[0].replace('ADD COLUMN ', '') for col in add_i18n])

            i18n_cmd  = 'CREATE TABLE "{table}_i18n" (\n'
            i18n_cmd += '   "locale" CHARACTER VARYING(5),\n'
            i18n_cmd += '   "{table}_id" {id_type} REFERENCES "{table}" ({pcol}),\n'
            i18n_cmd += '   {body},\n'
            i18n_cmd += '   CONSTRAINT "{table}_i18n_pkey" PRIMARY KEY ("{table}_id", "locale")\n'
            i18n_cmd += ') WITH (OIDS=FALSE);\n'
            if owner:
                i18n_cmd += 'ALTER TABLE "{table}_i18n" OWNER TO "{owner}";'

            i18n_cmd = i18n_cmd.format(table=model.schema().dbname(),
                                       id_type=id_type, pcol=pcol, body=i18n_body, owner=owner)

            cmd += '\n' + i18n_cmd

        return cmd, {}

PSQLStatement.registerAddon('CREATE', CREATE())