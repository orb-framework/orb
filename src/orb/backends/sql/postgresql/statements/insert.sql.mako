<%
    insert_columns = [column for column in columns if not (column.autoIncrement() or column.isTranslatable())]
    translate_columns = [column for column in columns if not column.autoIncrement() and column.isTranslatable()]
    insertions = []
    translations = []

    if translate_columns:
        __data__['output']['locale'] = options.locale

    count = len(records)
    for i, record in enumerate(records):
        values = record.recordValues()

        # add normal columns
        insertion = []
        for column in insert_columns:
            key = len(__data__['output'])
            insertion.append('%({0})s'.format(key))
            __data__['output'][str(key)] = __sql__.datastore().store(column, values[column.name()])
        insertions.append('({0})'.format(','.join(insertion)))

        # add translation columns
        if translate_columns:
            for locale in record.recordLocales():
                # define primary key information
                key = len(__data__['output'])
                translation = ['lastval()-{0}'.format(count-(i+1)), '%({0})s'.format(key)]
                __data__['output'][str(key)] = locale

                # add translatable column information
                found = False
                for column in translate_columns:
                    value = record.recordValue(column.name(), locale=locale)
                    if value is not None:
                        found = True
                    key = len(__data__['output'])
                    translation.append('%({0})s'.format(key))
                    __data__['output'][str(key)] = __sql__.datastore().store(column, value)

                if not found:
                    continue
                translations.append('({0})'.format(','.join(translation)))
%>
INSERT INTO "${schema.tableName()}" (
    ${','.join(['"{0}"'.format(column.fieldName()) for column in insert_columns])}
)
VALUES
    ${',\n    '.join(insertions)}
;
% if translations:
INSERT INTO "${schema.tableName()}_i18n" (
    "${schema.tableName()}_id", "locale", ${','.join(['"{0}"'.format(column.fieldName()) for column in translate_columns])}
)
VALUES
    ${',\n    '.join(translations)}
;
% endif