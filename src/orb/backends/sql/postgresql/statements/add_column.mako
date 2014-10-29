<%
# determine all the flags for this column
flags = []
Flags = orb.Column.Flags
for flag in column.iterFlags():
    flag_sql = __sql__.byName('Flag::{0}'.format(Flags(flag)))
    if flag_sql:
        flags.append(flag_sql)

# determine the SQL type and default length based on the column
type_name = column.columnTypeText()
type = __sql__.byName('Type::{0}'.format(type_name))
default_length = __sql__.byName('Length::{0}'.format(type_name))
max_length = column.maxlength() or default_length

if column.reference() and not column.referenceModel():
    raise RuntimeError('Invalid reference column: {0}::{1}'.format(column.schema().name(), column.name()))
%>

% if column.testFlag(column.Flags.AutoIncrement):
ADD COLUMN "${column.fieldName()}" SERIAL
% elif column.reference():
ADD COLUMN "${column.fieldName()}" ${type} REFERENCES "${column.referenceModel().schema().tableName()}"
% elif type == 'CHARACTER VARYING' and max_length:
ADD COLUMN "${column.fieldName()}" CHARACTER VARYING(${max_length}) ${' '.join(flags)}
% else:
ADD COLUMN "${column.fieldName()}" ${type} ${' '.join(flags)}
% endif