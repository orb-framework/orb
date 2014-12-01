<%
SELECT = __sql__.byName('SELECT')

# only select the primary keys if we're only interested in the row count
if not lookup.columns:
    lookup.columns = [col.name() for col in table.schema().primaryColumns()]

select_command = SELECT(table,
                        lookup=lookup,
                        options=options,
                        __data__=__data__)[0]

select_command = select_command.strip().strip(';')
%>
% if select_command:
SELECT COUNT(*) AS count
FROM (
    ${select_command}
) AS records;
% endif