<%
    import projex.text
    columns = []
    if column:
        index_name = projex.text.underscore(column.indexName().lstrip('by')) + '_idx'
        table_name = column.schema().dbname()
        unique = column.unique()
        col_table_name = table_name if not column.isTranslatable() else table_name + '_i18n'

        if not column.testFlag(column.Flags.CaseSensitive) and column.isString():
            columns.append('lower("{0}")'.format(column.fieldName()))
        else:
            columns.append('"{0}"'.format(column.fieldName()))
    else:
        index_name = projex.text.underscore(index.name().lstrip('by')) + '_idx'
        table_name = index.schema().dbname()
        unique = index.unique()
        cols = [index.schema().column(col_name) for col_name in index.columnNames()]
        translatable = {col.isTranslatable() for col in cols}

        # can only create a DB index on multiple columns if they're on the same table
        if len(translatable) == 1:
            if list(translatable)[0]:
                table_name += '_i18n'

            for col in cols:
                if not col.testFlag(col.Flags.CaseSensitive) and col.isString():
                    columns.append('lower("{0}")'.format(col.fieldName()))
                else:
                    columns.append('"{0}"'.format(col.fieldName()))
%>
% if columns:
% if checkExists:
DO $$
BEGIN
IF NOT EXISTS (
    SELECT  1
    FROM    pg_indexes
    WHERE   schemaname = 'public'
    AND     indexname = '${table_name}_${index_name}'
) THEN CREATE ${'UNIQUE' if unique else ''} INDEX ${table_name}_${index_name} ON "${table_name}" (${', '.join(columns)});
END IF;
END$$;
% else:
CREATE ${'UNIQUE' if unique else ''} INDEX ${table_name}_${index_name} ON "${table_name}" (${', '.join(columns)});
% endif
% else:
% endif