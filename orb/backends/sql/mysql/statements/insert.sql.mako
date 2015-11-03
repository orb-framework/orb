% if insertions['base']:
INSERT INTO `${table}` (
    ${','.join(['`{0}`'.format(column.fieldName()) for column in columns['base']])}
)
VALUES
    % for row in insertions['base'][:-1]:
    (${','.join(row)}),
    % endfor
    (${','.join(insertions['base'][-1])})
RETURNING id;
% endif

% if insertions['i18n']:
<% count = len(insertions['i18n']) %>
INSERT INTO `${table}_i18n` (
    `${table}_id`, `locale`, ${','.join(['`{0}`'.format(column.fieldName()) for column in columns['i18n']])}
)
VALUES
    % for i, row in enumerate(insertions['i18n'][:-1]):
    (LASTVAL() - ${count - (i+1)}, %(locale)s, ${','.join(row)}),
    %endfor
    (LASTVAL(), %(locale)s, ${','.join(insertions['i18n'][-1])})
RETURNING `${table}_id` AS `id`;
% endif