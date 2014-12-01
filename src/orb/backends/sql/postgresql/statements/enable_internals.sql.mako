% if not table:
SET unique_checks=${int(enabled)};
SET foreign_key_checks=${int(enabled)};
% else:
% if enabled:
ALTER TABLE "${table}" ENABLE KEYS;
% else:
ALTER TABLE "${table}" DISABLE KEYS;
% endif
% endif