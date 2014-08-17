% if schema is None:
SET unique_checks=${int(enabled)};
SET foreign_key_checks=${int(enabled)};
% else:
% if enabled:
ALTER TABLE "${schema.tableName()}" ENABLE KEYS;
% else:
ALTER TABLE "${schema.tableName()}" DISABLE KEYS;
% endif
% endif