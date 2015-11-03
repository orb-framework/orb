-- create the table
CREATE TABLE IF NOT EXISTS "${table}" (
    -- define the columns
    % for i, column in enumerate(columns['base'] + columns['primary']):
    ${ADD_COLUMN(column).replace('ADD COLUMN ', '')},
    % endfor

    % if columns['primary']:
    -- define the primary key constraint
    CONSTRAINT "${table}_pkey" PRIMARY KEY (${u','.join([QUOTE(col.fieldName()) for col in columns['primary']])})
    % endif
)
% if inherits:
INHERITS ("${inherits}")
% endif
WITH (OIDS=FALSE);
ALTER TABLE "${table}" OWNER TO "${owner}";

% if columns['i18n']:
-- create the translations table
CREATE TABLE "${table}_i18n" (
    -- define the columns
    "locale" CHARACTER VARYING(5),
    "${table}_id" BIGINT REFERENCES "${table}" (${u','.join([QUOTE(col.fieldName()) for col in columns['primary']])}) ON DELETE CASCADE,
    % for column in columns['translations']:
    ${ADD_COLUMN(column).replace('ADD COLUMN ', '')},
    % endfor
    
    -- define the constraints
    CONSTRAINT "${table}_i18n_pkey" PRIMARY KEY ("${table}_id", "locale")
)
WITH (OIDS=FALSE);
ALTER TABLE "${table}_i18n" OWNER TO "${owner}";
% endif
