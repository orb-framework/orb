<%
ADD_COLUMN = __sql__.byName('ADD_COLUMN')
ADD_CONSTRAINT = __sql__.byName('ADD_CONSTRAINT')

schema = table.schema()
table_name = schema.tableName()
pcols = [u'"{0}"'.format(pcol.fieldName()) for pcol in schema.primaryColumns()]

if not pcols:
    raise errors.DatabaseError('No primary keys defined.')

columns = []
translations = []
for column in sorted(schema.columns(recurse=False), key=lambda x: x.fieldName()):
    if column.isAggregate():
        continue
    elif column.isJoined():
        continue
    elif column.isProxy():
        continue
    elif column.isTranslatable():
        translations.append(column)
    elif column.primary() and schema.inherits():
        continue
    elif column.isReference():
        continue
    else:
        columns.append(column)
%>
-- create the table
CREATE TABLE IF NOT EXISTS "${table_name}" (
    -- define the columns
    % for i, column in enumerate(columns):
    ${ADD_COLUMN(column)[0].strip().replace('ADD COLUMN ', '')},
    % endfor
    -- define the constraints
    % if pcols:
    CONSTRAINT "${table_name}_pkey" PRIMARY KEY (${u','.join(pcols)})
    % endif
)
% if schema.inherits():
INHERITS "${schema.inheritsModel().schema().tableName()}"
% endif
WITH (OIDS=FALSE);
ALTER TABLE "${table_name}" OWNER TO "${__db__.username()}";

% if translations:
-- create the translations table
CREATE TABLE "${table_name}_i18n" (
    -- define the columns
    "locale" CHARACTER VARYING(5),
    "${table_name}_id" BIGINT REFERENCES "${table_name}" (${u','.join(pcols)}) ON DELETE CASCADE,
    % for translation in translations:
    ${ADD_COLUMN(translation)[0].strip().replace('ADD COLUMN ', '')},
    % endfor
    
    -- define the constraints
    CONSTRAINT "${table_name}_i18n_pkey" PRIMARY KEY ("${table_name}_id", "locale")
)
WITH (OIDS=FALSE);
ALTER TABLE "${table_name}_i18n" OWNER TO "${__db__.username()}";
% endif