<%
ADD_COLUMN = __sql__.byName('ADD COLUMN')
ADD_CONSTRAINT = __sql__.byName('ADD CONSTRAINT')

schema = table.schema()
table_name = schema.tableName()
pcols = [u'"{0}"'.format(pcol.fieldName()) for pcol in schema.primaryColumns()]

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
    else:
        columns.append(column)
%>
-- create the table
CREATE TABLE "${table_name}" (
    -- define the columns
    % for column in columns:
    ${ADD_COLUMN(column)[0].strip()},
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
CREATE TABLE "${table_name}__translation" (
    -- define the columns
    ADD COLUMN "tr_lang" CHARACTER VARYING(5),
    ADD COLUMN "${table_name}_id" REFERENCES "${table_name}" (${u','.join(pcols)}) ON DELETE CASCADE,
    % for translation in translations:
    ${ADD_COLUMN(translation)[0].strip()},
    % endfor
    
    -- define the constraints
    CONSTRAINT "${table_name}_translation_pkey" PRIMARY KEY ("${table_name}_id", "tr_lang")
)
WITH (OIDS=FALSE);
ALTER TABLE "${table_name}__translation" OWNER TO "${__db__.username()}";
% endif