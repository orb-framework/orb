<%
ADD_COLUMN = __sql__.byName('ADD_COLUMN')

table_name = schema.tableName()

translations = []
columns = []
constraints = []
pcols = ['"{0}"'.format(pcol.fieldName()) for pcol in schema.primaryColumns()]

for column in added:
    if column.isAggregate():
        continue
    elif column.isJoined():
        continue
    elif column.isProxy():
        continue
    elif column.isTranslatable():
        translations.append(ADD_COLUMN(column)[0].strip())
    else:
        columns.append(ADD_COLUMN(column)[0].strip())
%>
% if columns:
ALTER TABLE "${table_name}"
    ${',\n    '.join(columns)};
% endif

% if translations:
-- ensure the translation table exists (in case this is the first set of columns)
CREATE TABLE IF NOT EXISTS "${table_name}_i18n" (
    -- define the pkey columns
    "locale" CHARACTER VARYING(5),
    "${table_name}_id" BIGINT REFERENCES "${table_name}" (${u','.join(pcols)}) ON DELETE CASCADE,
    
    -- define the pkey constraints
    CONSTRAINT "${table_name}_i18n_pkey" PRIMARY KEY ("locale", "${table_name}_id")
) WITH (OIDS=FALSE);
ALTER TABLE "${table_name}_i18n" OWNER TO "${__db__.username()}";


-- add the missing columns to the translation table
ALTER TABLE "${table_name}_i18n"
    ${',\n    '.join(translations)};
% endif