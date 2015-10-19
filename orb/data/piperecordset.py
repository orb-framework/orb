""" Defines an piping system to use when accessing multi-to-multi records. """

from projex.lazymodule import lazy_import
from orb import errors

from .recordset import RecordSet

orb = lazy_import('orb')


class PipeRecordSet(RecordSet):
    def __init__(self,
                 records,
                 source,
                 pipeTable=None,
                 sourceColumn='',
                 targetColumn=''):

        super(PipeRecordSet, self).__init__(records, sourceColumn=sourceColumn, source=source)

        # define additional properties
        self._pipeTable = pipeTable
        self._targetColumn = targetColumn

    def createRecord(self, values, **options):
        # link an existing column to this recordset
        if self._targetColumn in values:
            record = self.table()(values[self._targetColumn])
            self.addRecord(record)
            return record

        # otherwise, create a new record based on the target table and link it
        else:
            record = self.table().createRecord(values, **options)
            self.addRecord(record)
            return record

    def addRecord(self, record, **options):
        """
        Adds a new record for this pipe set.  The inputted record should refer
        to the value for the target column and be an instance of the
        target type.
        
        :param      record | <orb.Table>
        
        :return     <orb.Table> || None | instance of the pipe table
        """
        # make sure we have a valid table and record
        table = self.table()
        if not (table and record and record.isRecord() and isinstance(record, table)):
            raise errors.OrbError('Invalid arguments for creating a record.')

        # make sure we have a pipe table
        if not self._pipeTable:
            raise errors.OrbError('No pipe table defined for {0}'.format(self))

        pipe = self._pipeTable
        unique = options.pop('uniqueRecord', True)

        if unique:
            q = orb.Query(pipe, self.sourceColumn()) == self.source()
            q &= orb.Query(pipe, self._targetColumn) == record

            if pipe.selectFirst(where=q):
                msg = 'A record already exists for {0} and {1}'.format(self.sourceColumn(), self._targetColumn)
                raise errors.DuplicateEntryFound(msg)

        values = {self.sourceColumn(): self.source(), self._targetColumn: record}

        return pipe.createRecord(values, **options)

    def clear(self, **options):
        """
        Clears all the records through the pipeset based on the inputted
        parameters.
        
        :return     <int>
        """
        pipe = self._pipeTable
        if not pipe:
            return 0

        q = orb.Query(pipe, self.sourceColumn()) == self.source()

        where = options.pop('where', None)
        q &= where

        for key, value in options.items():
            q &= orb.Query(pipe, key) == value

        return pipe.select(where=q).remove()

    def hasRecord(self, record, **options):
        """
        Checks to see if the given record is in the record set for this
        instance.
        
        :param      record | <orb.Table>
        
        :return     <bool>
        """
        table = self.table()
        if not (table and record and record.isRecord() and isinstance(record, table)):
            return False

        where = self.query() & (orb.Query(table) == record)
        return table.selectFirst(where=where) is not None

    def removeRecord(self, record, **options):
        """
        Removes the record from this record set and from the database based
        on the piping information.
        
        :param      record | <orb.Table>
        
        :return     <int> | number of records removed
        """
        table = self.table()
        if not (table and record and record.isRecord() and isinstance(record, table)):
            return 0

        pipe = self._pipeTable
        if not pipe:
            return 0

        options.pop('uniqueRecord', True)

        q = orb.Query(pipe, self.sourceColumn()) == self.source()
        q &= orb.Query(pipe, self._targetColumn) == record

        for key, value in options.items():
            q &= orb.Query(pipe, key) == value

        record = pipe.select(where=q).first()
        if record is not None:
            return record.remove()
        else:
            return 0

    def remove(self, **options):
        """
        Removes all the links within this set.  This will remove
        from the Pipe table and not from the source record.
        """
        pipe = self._pipeTable
        if not pipe:
            return 0

        q = orb.Query(pipe, self.sourceColumn()) == self.source()

        for key, value in options.items():
            q &= orb.Query(pipe, key) == value

        return pipe.select(where=q).remove()

    def update(self, **values):
        if 'ids' in values:
            return self.setRecords(values['ids'])
        elif 'records' in values:
            return self.setRecords([x.get('id') for x in values['records'] if x.get('id')])
        else:
            raise errors.OrbError('Invalid update parameters: {0}'.format(values))

    def setRecords(self, records):
        """
        Updates the linked records for this pipe by removing records not within the inputted
        record set and adding any missing ones.

        :param      records | <orb.RecordSet> || [<orb.Table>, ..] || [<int>, ..]

        :return     <int> added, <int> removed
        """
        pipe = self._pipeTable
        if not pipe:
            raise orb.errors.TableNotFound(self)
        elif not isinstance(records, (orb.RecordSet, list, tuple)):
            records = [records]

        # determine the records to sync
        curr_ids = self.ids()
        record_ids = records.ids() if isinstance(records, orb.RecordSet) else [int(r) for r in records if r]
        remove_ids = set(curr_ids) - set(record_ids)
        add_ids = set(record_ids) - set(curr_ids)

        # remove old records
        if remove_ids:
            q = orb.Query(pipe, self.sourceColumn()) == self.source()
            q &= orb.Query(pipe, self._targetColumn).in_(remove_ids)
            pipe.select(where=q).remove()

        # create new records
        if add_ids:
            # noinspection PyShadowingBuiltins
            rset = orb.RecordSet([pipe(**{self.sourceColumn(): self.source(), self._targetColumn: id})
                                  for id in add_ids])
            rset.commit()

        return len(add_ids), len(remove_ids)

