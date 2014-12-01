% if added['standard']:
-- update the table
ALTER TABLE "${table}"
    % for column in added['standard'][:-1]:
    ${ADD_COLUMN(column)},
    % endfor
    ${ADD_COLUMN(column)}
;
% endif

% if added['i18n']:
-- ensure the translation table exists (in case this is the first set of columns)
CREATE TABLE IF NOT EXISTS "${table}_i18n" (
    -- define the pkey columns
    "locale" CHARACTER VARYING(5),
    "${table}_id" BIGINT REFERENCES "${table}" (${u','.join(pcols)}) ON DELETE CASCADE,
    
    -- define the pkey constraints
    CONSTRAINT "${table}_i18n_pkey" PRIMARY KEY ("locale", "${table}_id")
) WITH (OIDS=FALSE);
ALTER TABLE "${table}_i18n" OWNER TO "${owner}";


-- add the missing columns to the translation table
ALTER TABLE "${table}_i18n"
    % for column in added['i18n'][:-1]:
    ${ADD_COLUMN(column)},
    % endfor
    ${ADD_COLUMN(column)}
% endif