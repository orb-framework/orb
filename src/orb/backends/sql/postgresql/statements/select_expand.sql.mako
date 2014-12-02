% if pipe:
    <%
        source_table = pipe.sourceReferenceModel()
        join_table = pipe.pipeReferenceModel()
        source_column = join_table.schema().column(pipe.sourceColumn())
        target_column = join_table.schema().column(pipe.targetColumn())
        target_table = pipe.targetReferenceModel()
    %>
    % if pipe.name() in lookup.expand:
    (
        % if target_table.schema().hasTranslations():
            SELECT ARRAY_AGG(ROW_TO_JSON(row)) FROM (
                SELECT target_table.*, i18n.*
                FROM "${target_table.schema().tableName()}" AS target_table
                LEFT JOIN "${target_table.schema().tableName()}_i18n" AS i18n
                ON i18n.locale = '${options.locale}' AND i18n."${target_table.schema().tableName()}_id" = target_table.id
                WHERE target_table.id IN (
                    SELECT join_table."${target_column.fieldName()}"
                    FROM "${join_table.schema().tableName()}" AS join_table
                    WHERE join_table."${source_column.fieldName()}" = "${source_table.schema().tableName()}".id
                )
                GROUP BY target_table.id, i18n."${target_table.schema().tableName()}_id", i18n.locale
            ) row
        % else:
            SELECT ARRAY_AGG(ROW_TO_JSON(target_table.*))
            FROM "${target_table.schema().tableName()}" AS target_table
            WHERE target_table.id IN (
                SELECT join_table."${target_column.fieldName()}"
                FROM "${join_table.schema().tableName()}" AS join_table
                WHERE join_table."${source_column.fieldName()}" = "${source_table.schema().tableName()}".id
            )
        % endif
    ) AS ${pipe.name()}
    % elif pipe.name() + '.ids' in lookup.expand:
    (
        SELECT ARRAY_AGG(target_table.id)
        FROM "${target_table.schema().tableName()}" AS target_table
        WHERE target_table.id IN (
            SELECT join_table."${target_column.fieldName()}"
            FROM "${join_table.schema().tableName()}" AS join_table
            WHERE join_table."${source_column.fieldName()}" = "${source_table.schema().tableName()}".id
        )
    ) AS ${pipe.name()}_ids
    % elif pipe.name() + '.count' in lookup.expand:
    (
        SELECT COUNT(target_table.*)
        FROM "${target_table.schema().tableName()}" AS target_table
        WHERE target_table.id IN (
            SELECT join_table."${target_column.fieldName()}"
            FROM "${join_table.schema().tableName()}" AS join_table
            WHERE join_table."${source_column.fieldName()}" = "${source_table.schema().tableName()}".id
        )
    ) AS ${pipe.name()}_count
    % endif

% elif reverseLookup:
    <%
        source_schema = reverseLookup.schema()
        source_column = reverseLookup
        ref_schema = reverseLookup.referenceModel().schema()
    %>
    % if reverseLookup.reversedName() in lookup.expand:
    (
        % if source_schema.hasTranslations():
            SELECT ARRAY_AGG(ROW_TO_JSON(row)) FROM (
                SELECT source_table.*, i18n.*
                FROM "${source_schema.tableName()}" AS target_table
                LEFT JOIN "${source_schema.tableName()}_i18n" AS i18n
                ON i18n.locale = '${options.locale}' AND i18n."${source_schema.tableName()}_id" = ref_table.id
                WHERE source_table."${source_column.fieldName()}" = "${ref_schema.tableName()}".id
                GROUP BY source_table.id, i18n."${source_schema.tableName()}_id", i18n.locale
            ) row
        % else:
            SELECT ARRAY_AGG(ROW_TO_JSON(source_table.*))
            FROM "${source_schema.tableName()}" AS source_table
            WHERE source_table."${source_column.fieldName()}" = "${ref_schema.tableName()}".id
        % endif
    ) AS ${reverseLookup.reversedName()}
    % elif reverseLookup.reversedName() + '.ids' in lookup.expand:
    (
        SELECT ARRAY_AGG(source_table.id)
        FROM "${source_schema.tableName()}" AS source_table
        WHERE source_table."${source_column.fieldName()}" = "${ref_schema.tableName()}".id
    ) AS ${reverseLookup.reversedName()}_ids
    % elif reverseLookup.reversedName() + '.count' in lookup.expand:
    (
        SELECT COUNT(source_table.*)
        FROM "${source_schema.tableName()}" AS source_table
        WHERE source_table."${source_column.fieldName()}" = "${ref_schema.tableName()}".id
    ) AS ${reverseLookup.reversedName()}_count
    % endif

% elif column:
    <%
        reference = column.referenceModel()
    %>
    (
        SELECT ROW_TO_JSON(join_table.*)
        FROM "${reference.schema().tableName()}" AS join_table
        WHERE join_table.id = "${column.schema().tableName()}"."${column.fieldName()}"
    ) AS "${column.name()}"
% endif