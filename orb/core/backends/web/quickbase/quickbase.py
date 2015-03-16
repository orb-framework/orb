""" Defines the backend connection class for Quickbase databases. """

import datetime
import orb
import logging
import projex.errors
import projex.text

from orb import Query as Q
from orb import errors
from projex.text import nativestring as nstr
from xml.etree import ElementTree

try:
    import urllib2
except ImportError:
    urllib2 = None

log = logging.getLogger(__name__)

# ----------------------------------------------------------------------
#                       DEFINE ERROR CLASSES
# ---------------------------------------------------------------------


class ConnectionError(errors.DatabaseError):
    pass


class ResponseError(errors.DatabaseError):
    pass


class UnknownError(errors.DatabaseError):
    pass


# -----------------------------------------------------------------------------

# noinspection PyAbstractClass
class Quickbase(orb.Connection):
    # map the default operator types to a SQL operator
    OpMap = {
        Q.Op.Is: 'EX',
        Q.Op.IsNot: 'XEX',
        Q.Op.LessThan: 'LT',
        Q.Op.LessThanOrEqual: 'LTE',
        Q.Op.GreaterThan: 'GT',
        Q.Op.GreaterThanOrEqual: 'GTE',
        Q.Op.Contains: 'CT',
        Q.Op.DoesNotContain: 'XCT',
        Q.Op.IsIn: 'HAS',
        Q.Op.IsNotIn: 'XHAS',
        Q.Op.Startswith: 'SW',
        Q.Op.Endswith: 'EW',
        Q.Op.Before: 'BF',
        Q.Op.After: 'AF',
        Q.Op.Between: 'IR',
    }

    def __init__(self, database):
        super(Quickbase, self).__init__(database)

        # define custom properties
        self._ticket = None
        self._timeout = 30

    def _fieldIds(self, schema):
        """
        Retreives the field ids from the inputted schema, generating them if
        necessary.
        
        :param      schema | <orb.TableSchema>
        
        :return     {<str> fieldName: <str> id, ..}
        """
        # map to the id values for the fields
        field_ids = schema.property('quickbase_field_ids')
        if not field_ids:
            field_ids = {}
            col_map = dict([(c.displayName(), c.fieldName())
                            for c in schema.columns(includeProxies=False)])
            table_id = schema.dbname()
            xresp = self.request(table_id + '?a=td', 'GetSchema')
            xtable = xresp.find('table')
            xfields = xtable.find('fields')
            for xfield in xfields:
                fieldName = col_map.get(xfield.find('label').text)
                if not fieldName:
                    continue

                field_ids[fieldName] = xfield.get('id', '0')

            schema.setProperty('quickbase_field_ids', field_ids)
        return field_ids

    def _select(self, table_or_join, lookup, options):
        """
        Performs the database lookup and returns the raw pymongo information
        for processing.
        
        :param      table_or_join | <subclass of orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.ContextOptions>
        
        :return     <variant>
        """
        if not self.open():
            return []

        schemas = []

        # initialize a lookup from a table
        if orb.Table.typecheck(table_or_join):
            schemas.append((table_or_join.schema(), lookup.columns))

        # intialize a lookup from a join
        elif orb.Join.typecheck(table_or_join):
            log.warning('Joining is not yet supported for Quickbase')
            return []

        # make sure we have a valid query
        else:
            raise errors.QueryInvalid('Invalid select option: {0}'.format(table_or_join))

        qb_opts = []

        if lookup.limit:
            qb_opts.append('num-%i' % lookup.limit)
        if lookup.start:
            qb_opts.append('skip-%i' % lookup.start)

        qb_options = '.'.join(qb_opts)
        output = []
        for schema, columns in schemas:
            table_id = schema.dbname()
            field_ids = self._fieldIds(schema)

            # generate the request parameters
            request = {'includeRids': 1}
            if options:
                request['options'] = qb_options

            # collect specific columns
            if lookup.columns is not None:
                fields = [schema.column(c).fieldName() for c in lookup.columns]
                fids = [field_ids.get(x, '0') for x in fields]
                request['clist'] = '.'.join(fids)

            # collect specific entries
            if lookup.where:
                where = lookup.where.expandShortcuts(schema)
                request['query'] = self.queryCommand(schema, where)

            # convert the sorting keys
            if lookup.order:
                sort = []
                direc = 'sortorder-A'
                for col, direction in lookup.order:
                    if direction == 'desc':
                        direc = 'sortorder-D'
                    fid = field_ids.get(schema.column(col).fieldName(), '0')
                    sort.append(fid)
                request['slist'] = '.'.join(sort)

                if qb_options:
                    request['options'] += '.%s' % direc
                else:
                    request['options'] = direc

            # collect the records
            xresponse = self.request(table_id + '?a=td', 'DoQuery', request)

            # look for error data
            error_code = xresponse.find('errcode').text
            error_text = xresponse.find('errtext').text

            if error_code != '0':
                raise errors.DatabaseError(error_text)

            # extract the record data
            records = []
            for xrecord in xresponse:
                if not xrecord.tag == 'record':
                    continue

                record = {'_id': int(xrecord.get('rid'))}
                for xfield in xrecord:
                    fname = xfield.tag
                    fval = xfield.text
                    col = schema.column(fname)
                    if not col:
                        continue

                    fname = col.name()
                    ftype = col.columnType()

                    if ftype == orb.ColumnType.Bool:
                        fval = fval == '1'

                    elif ftype in (orb.ColumnType.Integer,
                                   orb.ColumnType.Enum,
                                   orb.ColumnType.ForeignKey):
                        try:
                            fval = int(fval)
                        except:
                            fval = 0

                    elif ftype in (orb.ColumnType.Double, orb.ColumnType.Decimal):
                        try:
                            fval = float(fval)
                        except:
                            fval = 0.0

                    elif ftype == orb.ColumnType.Date:
                        try:
                            fval = datetime.datetime.strptime(fval, '%Y-%m-%d')
                            fval = fval.date()
                        except:
                            fval = None

                    elif ftype == orb.ColumnType.Datetime:
                        try:
                            fval = datetime.datetime.strptime(fval,
                                                              '%Y-%m-%d %H:%M:%S')
                        except:
                            fval = None

                    elif ftype == orb.ColumnType.Time:
                        try:
                            fval = datetime.datetime.strptime(fval, '%H:%M:%S')
                            fval = fval.time()
                        except:
                            fval = None

                    record[fname] = fval

                records.append(record)

            output.append((schema, records))
        return output

    def count(self, table_or_join, lookup, options):
        """
        Returns the count for the results based on the given query.
        
        :return     <int>
        """
        if not self.open():
            return 0

        # initialize a lookup from a table
        if not orb.Table.typecheck(table_or_join):
            log.warning('Joining is not yet supported for Quickbase')
            return []

        schema = table_or_join.schema()
        table_id = schema.dbname()
        if not lookup.where:
            xresponse = self.request(table_id + '?a=td', 'GetNumRecords')
            return int(xresponse.find('num_records').text)
        else:
            opts = {}
            where = lookup.where.expandShortcuts(table_or_join)
            opts['query'] = self.queryCommand(schema, where)
            xresponse = self.request(table_id + '?a=td', 'DoQueryCount', opts)
            return int(xresponse.find('numMatches').text)

    def isConnected(self):
        """
        Returns whether or not this connection is established.
        
        :return     <bool>
        """
        return self._ticket is not None

    def open(self):
        """
        Opens a new database connection to the datbase defined
        by the inputted database.
        
        :return     <bool> success
        """
        if not self._database:
            self._failed = True
            raise errors.DatabaseNotFound()

        elif self._ticket:
            return True

        user = self._database.username()
        pword = self._database.password()

        request = {'username': user, 'password': pword}
        response = self.request('main',
                                'Authenticate',
                                request,
                                required=['ticket', 'userid'],
                                useTicket=False)

        self._ticket = response['ticket']
        self._qb_user_id = response['userid']

        return True

    def queryCommand(self, schema, query):
        """
        Converts the inputted query object to a SQL query command.
        
        :param      schema  | <orb.TableSchema> || None
        :param      query   | <orb.Query>
        
        :return     <str> | query
        """
        if query.isNull():
            log.debug('Quickbase.queryCommand: NULL QUERY.')
            return ''

        # load query compoundss
        if orb.QueryCompound.typecheck(query):
            # extract the rest of the query information
            output = []

            # determine the joining operator
            join = ' AND '
            if query.operatorType() == orb.QueryCompound.Op.Or:
                join = ' OR '

            # generate the queries
            for q in query.queries():
                q_str = self.queryCommand(schema, q)
                if q_str:
                    output.append(q_str)

            return join.join(output)

        # load Query objects
        # initialize the field query objects
        if query.table():
            schema = query.table().schema()

        # make sure we have a schema to work with
        elif not schema:
            raise errors.QueryInvalid(query)

        value = query.value()
        dbname = schema.dbname()
        op = query.operatorType()
        colname = query.columnName()
        col = schema.column(colname)

        if not col:
            raise errors.ColumnNotFound(dbname, colname)

        # extract the primary key information
        if orb.Table.recordcheck(value) or orb.View.recordcheck(value):
            value = self.recordCommand(col, value)

        # extract the primary key information for a list of items
        elif type(value) in (list, tuple):
            value = [self.recordCommand(col, entry) for entry in value]
            value = ','.join([str(x) for x in value])

        # extract the primary key information from a record set
        elif isinstance(value, orb.RecordSet):
            field = orb.system.settings.primaryField()
            value = ','.join([str(x) for x in value.values(field)])

        field = col.fieldName()
        try:
            return "{'%s'.%s.'%s'}" % (field, self.OpMap[op], value)
        except KeyError:
            return ''

    # noinspection PyUnusedLocal
    @staticmethod
    def recordCommand(column, value):
        """
        Converts the inputted value from a record instance to a mongo id pointer,
        provided the value is a table type.  If not, the inputted value is
        returned unchanged.
        
        :param      column | <orb.Column>
                    value  | <variant>
        
        :return     <variant>
        """
        # handle conversions of non record values to mongo object ids when
        # necessary
        if not (orb.Table.recordcheck(value) or orb.View.recordcheck(value)):
            return value

        pkey = value.primaryKey()
        if not pkey:
            raise errors.PrimaryKeyNotDefined(value)

        if type(pkey) in (list, tuple, set):
            if len(pkey) == 1:
                pkey = pkey[0]
            else:
                pkey = tuple(pkey)

        return nstr(pkey)

    def request(self,
                dbname,
                action,
                request=None,
                required=None,
                useTicket=True,
                useToken=True):
        """
        Does a Quickbase query based on the inputted action information.
        
        :param     action    | <str> | 'API_' will be prepended automatically
                   request   | {<str> key: <variant> value}
                   required  | [<str>, ..] || None
                   useTicket | <bool>
        
        :return     [<str>, ..] | response
        """
        # generate the URL for the database
        url = self.database().host() + '/db/' + dbname

        if request is None:
            request = {}

        # include the ticket if required
        if useTicket:
            request['ticket'] = self._ticket

        if useToken:
            request['apptoken'] = self.database().applicationToken()

        request['encoding'] = 'UTF-8'

        # generate the header and request information
        data = self.buildRequest(**request)
        headers = {
            'Content-Type': 'application/xml',
            'Accept-Charset': 'utf-8',
            'QUICKBASE-ACTION': 'API_' + action,
        }

        # debug the lookup
        log.debug(url)
        log.debug(nstr(headers))
        log.debug(projex.text.encoded(data))

        # create the request
        request = urllib2.Request(url, data, headers)
        try:
            f = urllib2.urlopen(request, timeout=self._timeout)
            response = f.read()
        except urllib2.HTTPError as error:
            try:
                response = error.read()
            except IOError:
                response = None
            raise ConnectionError(nstr(error))
        except urllib2.URLError as error:
            raise ConnectionError(nstr(error))
        except Exception as error:
            raise UnknownError(nstr(error))

        # Parse the response XML
        try:
            response.decode('utf-8')
        except UnicodeError:
            # Quickbase sometimes returns cp1252 even when ask for utf-8, fix it
            response = response.decode('cp1252').encode('utf-8')
        try:
            parsed = ElementTree.XML(response)
        except SyntaxError as error:
            raise ResponseError(nstr(error))

        # Ensure it's not a QuickBase error
        error_code = parsed.find('errcode')
        if error_code is None:
            raise ResponseError('"errcode" not in response')
        try:
            error_code = int(error_code.text)
        except ValueError:
            raise ResponseError('"errcode" not an integer')
        if error_code != 0:
            error_text = parsed.find('errtext')
            error_text = error_text.text if error_text is not None else '[no error text]'
            raise ResponseError(error_text)

        if required:
            # Build dict of required response fields caller asked for
            values = {}
            for field in required:
                value = parsed.find(field)
                if value is None:
                    err = '"{0}" not in response'.format(field)
                    raise ResponseError(err)
                values[field] = value.text or ''
            return values
        else:
            # Return parsed XML directly
            return parsed

    def select(self, table_or_join, lookup, options):
        """
        Selects the records from the database for the inputted table or join
        instance based on the given lookup and options.
                    
        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.ContextOptions>
        
        :return     [<variant> result, ..]
        """
        raw = self._select(table_or_join, lookup, options)

        output = []
        for schema, results in raw:
            for result in results:
                # force the primary column to use the object id
                db_result = {}
                for field, value in result.items():
                    col = schema.column(field)
                    if col:
                        db_result[col.name()] = value
                    else:
                        db_result[field] = value

                output.append(db_result)

        return output

    @staticmethod
    def buildRequest(**fields):
        r"""
        Builds a QuickBase request XML with given fields. Fields can be straight
        key=value, or if value is a 2-tuple it represents (attr_dict, value), 
        or if value is a list of values or 2-tuples the output will 
        contain multiple entries.
        """
        request = ElementTree.Element('qdbapi')

        # noinspection PyShadowingNames
        def add_sub_element(field, value):
            if isinstance(value, tuple):
                attrib, value = value
                attrib = dict((k, nstr(v)) for k, v in attrib.iteritems())
            else:
                attrib = {}
            sub_element = ElementTree.SubElement(request, field, **attrib)
            sub_element.text = nstr(value)

        for field, values in fields.iteritems():
            if not isinstance(values, list):
                values = [values]
            for value in values:
                add_sub_element(field, value)

        string = ElementTree.tostring(request, encoding='UTF-8')
        return string


if urllib2:
    orb.Connection.registerAddon('Quickbase', Quickbase)

