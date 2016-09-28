"""
Tests for the orb.errors module
"""

def test_generic_orb_error():
    import orb

    # ensure basic orb error creation works
    err = orb.errors.OrbError(u'I can put any error I want here.')
    assert isinstance(err, StandardError)


def test_database_error():
    import orb

    err = orb.errors.DatabaseError()
    assert isinstance(err, orb.errors.OrbError)
    assert err.message == u'Unknown database error occurred'

    msg = u'I can put a custom message'
    err = orb.errors.DatabaseError(msg)
    assert err.message == msg


def test_datastore_error():
    import orb

    err = orb.errors.DataStoreError(u'Some message')
    assert isinstance(err, orb.errors.OrbError)


def test_schema_error():
    import orb

    err = orb.errors.SchemaError(u'Some mssage')
    assert isinstance(err, orb.errors.OrbError)

    templ = '{column} not on {schema}'
    err = orb.errors.SchemaError(templ, schema='User', column='username')
    assert err.message == 'username not on User'

    schema = orb.Schema(name='User')
    column = orb.Column(name='username')

    err = orb.errors.SchemaError(templ, schema=schema, column=column)
    assert err.message == 'username not on User'


def test_validation_error():
    import orb

    err = orb.errors.ValidationError(u'Some message')
    assert isinstance(err, orb.errors.OrbError)
    assert err.context == ''

    err = orb.errors.ValidationError(u'Some message', context=u'some context')
    assert isinstance(err, orb.errors.OrbError)
    assert err.context == u'some context'

def test_backend_not_found_error():
    import orb

    err = orb.errors.BackendNotFound('postgres')
    assert isinstance(err, orb.errors.OrbError)
    assert err.message == u'Could not find postgres backend'


def test_cannot_delete_error():
    import orb

    err = orb.errors.CannotDelete(u'Could not remove record')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)


def test_column_not_found_error():
    import orb

    err = orb.errors.ColumnNotFound(schema='User', column='username')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.SchemaError)
    assert err.message == u'Did not find username column on User'


def test_column_read_only_error():
    import orb

    err = orb.errors.ColumnReadOnly(schema='User', column='username')
    assert isinstance(err, orb.errors.OrbError)
    assert err.message == u'username of User is a read-only column'

    column = orb.Column(name='username')
    err = orb.errors.ColumnReadOnly(schema='User', column=column)
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.SchemaError)
    assert err.message == u'username of User is a read-only column'


def test_column_validation_error():
    import orb

    column = orb.Column(name='username')
    err = orb.errors.ColumnValidationError(column, 'failed')

    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.ValidationError)
    assert err.context == 'username'
    assert err.column == column


def test_connection_failed_error():
    import orb

    err = orb.errors.ConnectionFailed()
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert err.message == u'Failed to connect to database'


def test_connection_lost_error():
    import orb

    err = orb.errors.ConnectionLost()
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert err.message == u'Connection was lost to the database.  Please retry again soon'


def test_database_not_found_error():
    import orb

    err = orb.errors.DatabaseNotFound()
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert err.message == u'No database was found'


def test_duplicate_column_found_error():
    import orb

    err = orb.errors.DuplicateColumnFound(schema='User', column='username')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.SchemaError)
    assert err.message == u'username of User is already defined and cannot be duplicated'


def test_duplicate_entry_found_error():
    import orb

    err = orb.errors.DuplicateEntryFound('user 1 already exists')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert err.message == 'user 1 already exists'


def test_dry_run_error():
    import orb

    err = orb.errors.DryRun()
    assert isinstance(err, orb.errors.DryRun)


def test_id_not_found_error():
    import orb

    err = orb.errors.IdNotFound(schema='User')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.SchemaError)
    assert err.message == u'No id column found for User model'

def test_interruption_error():
    import orb

    err = orb.errors.Interruption()
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert err.message == u'Database operation was interrupted'


def test_context_error():
    import orb

    err = orb.errors.ContextError(u'failed to generate context')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.ValidationError)
    assert err.message == u'failed to generate context'


def test_invalid_reference_error():
    import orb

    err = orb.errors.InvalidReference('created_by', expects='User', received='UserRole')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.ValidationError)
    assert err.message == u'created_by expects User records, not UserRole'

def test_column_type_not_found_error():
    import orb

    err = orb.errors.ColumnTypeNotFound('String')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.SchemaError)


def test_empty_command_error():
    import orb

    err = orb.errors.EmptyCommand()
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)


def test_invalid_index_arguments_error():
    import orb

    index = orb.Index(name='byName')

    err = orb.errors.InvalidIndexArguments(index, 'missing name')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.ValidationError)
    assert err.message == 'missing name'
    assert err.context == index.name()
    assert err.index == index


def test_query_error():
    import orb

    err = orb.errors.QueryError(u'Failed to load')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert err.message == u'Failed to load'


def test_query_failed_error():
    import orb

    err = orb.errors.QueryFailed(
        'SELECT * FROM users',
        {},
        'No table named users'
    )
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert isinstance(err, orb.errors.QueryError)
    assert err.message == u'Query was:\n\n' \
                          u'SELECT * FROM users\n\n' \
                          u'Data: {}\n\n' \
                          u'Error: No table named users'


def test_query_is_invalid_error():
    import orb

    err = orb.errors.QueryInvalid(u'invalid query')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert isinstance(err, orb.errors.QueryError)
    assert err.message == u'invalid query'


def test_query_is_null_error():
    import orb

    err = orb.errors.QueryIsNull()
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert isinstance(err, orb.errors.QueryError)
    assert err.message == u'This query will result in no items'


def test_query_timeout_error():
    import orb

    err = orb.errors.QueryTimeout(
        query=u'SELECT * FROM users',
        msecs=1000
    )
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.DatabaseError)
    assert isinstance(err, orb.errors.QueryError)
    assert err.message == u'The server cancelled the query because it was taking too long'
    assert err.query == u'SELECT * FROM users'
    assert err.msecs == 1000


def test_record_not_found_error():
    import orb

    err = orb.errors.RecordNotFound(schema='User', column='1')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.SchemaError)
    assert err.message == u'Could not find record User(1)'


def test_search_engine_not_found_error():
    import orb

    err = orb.errors.SearchEngineNotFound('elastic')
    assert isinstance(err, orb.errors.OrbError)
    assert err.message == u'Missing search engine: elastic'


def test_model_not_found_error():
    import orb

    err = orb.errors.ModelNotFound(schema='User')
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.SchemaError)
    assert err.message == u'Could not find User model'


def test_value_out_of_range_error():
    import orb

    err = orb.errors.ValueOutOfRange('count', 0)
    assert isinstance(err, orb.errors.OrbError)
    assert isinstance(err, orb.errors.ValidationError)
    assert err.message == u'0 for count is out of range.'

    err = orb.errors.ValueOutOfRange('count', 0, minimum=1)
    assert err.message == u'0 for count is out of range.  Value must be greater than 1'

    err = orb.errors.ValueOutOfRange('count', 2, maximum=1)
    assert err.message == u'2 for count is out of range.  Value must be less than 1'

    err = orb.errors.ValueOutOfRange('count', 5, minimum=1, maximum=3)
    assert err.message == u'5 for count is out of range.  Value must be between 1 and 3'