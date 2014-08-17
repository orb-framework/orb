<%
ADD_COLUMN = __sql__.byName('ADD COLUMN')

schema = table.schema()
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
        translations.append(column)
    else:
        columns.append(column)
%>
% if columns:
-- alter the table
ALTER TABLE "${table_name}" (
    -- define the columns
    % for column in columns:
    ${ADD_COLUMN(column)[0].strip()},
    % endfor
);
% endif

% if translations:
-- ensure the translation table exists (in case this is the first set of columns)
CREATE TABLE "${table_name}__translation" IF NOT EXISTS (
    -- define the pkey columns
    ADD COLUMN "tr_lang" CHARACTER VARYING(5),
    ADD COLUMN "${table_name}_id" REFERENCES "${table_name}" (${u','.join(pcols)}) ON DELETE CASCADE,
    
    -- define the pkey constraints
    CONSTRAINT "${table_name}_translation_pkey" PRIMARY KEY ("tr_lang", "${table_name}_id")
) WITH (OIDS=FALSE);
ALTER TABLE "${table_name}__translation" OWNER TO "${__db__.username()}";


-- add the missing columns to the translation table
ALTER TABLE "${table_name}__translation" (
    -- define the columns
    % for translation in translations:
    ${ADD_COLUMN(translation)[0].strip()},
    % endfor
);
% endif