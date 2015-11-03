% if added['base']:
-- update the table
ALTER TABLE `${table}`
    % for column in added['base'][:-1]:
    ${ADD_COLUMN(column)},
    % endfor
    ${ADD_COLUMN(added['base'][-1])}
;
% endif

% if added['i18n']:
-- ensure the translation table exists (in case this is the first set of columns)
CREATE TABLE IF NOT EXISTS `${table}_i18n` (
    -- define the pkey columns
    `locale` CHARACTER VARYING(5),
    `${table}_id` BIGINT REFERENCES `${table}` (${u','.join([QUOTE(col.fieldName()) for col in added['primary']])}) ON DELETE CASCADE,
    
    -- define the pkey constraints
    CONSTRAINT `${table}_i18n_pkey` PRIMARY KEY (`locale`, `${table}_id`)
) WITH (OIDS=FALSE);
ALTER TABLE `${table}_i18n` OWNER TO `${owner}`;


-- add the missing columns to the translation table
ALTER TABLE `${table}_i18n`
    % for column in added['i18n'][:-1]:
    ${ADD_COLUMN(column)},
    % endfor
    ${ADD_COLUMN(added['i18n'][-1])}
;
% endif

## create any indexes for this new table
% for column in schema.columns():
% if column.indexed() and not column.primary():
${CREATE_INDEX(column, checkExists=True, GLOBALS=GLOBALS, IO=IO)}
% endif
% endfor
% for index in schema.indexes():
${CREATE_INDEX(index, checkExists=True, GLOBALS=GLOBALS, IO=IO)}
% endfor
