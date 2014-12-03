% if column.testFlag(column.Flags.AutoIncrement):
ADD COLUMN "${field}" SERIAL
% elif reference:
ADD COLUMN "${field}" ${type} REFERENCES "${reference}"
% elif type == 'CHARACTER VARYING' and max_length:
ADD COLUMN "${field}" CHARACTER VARYING(${max_length}) ${' '.join(flags)}
% else:
ADD COLUMN "${column.fieldName()}" ${type} ${' '.join(flags)}
% endif