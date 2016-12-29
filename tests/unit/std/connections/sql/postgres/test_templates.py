from __future__ import print_function


# alter table sql
def test_render_alter_table(pg_conn, sql_equals, User):
    add = {
        'fields': [User.schema().column('username'), User.schema().column('password')],
        'indexes': [User.schema().index('by_username')]
    }
    sql, _ = pg_conn.render_alter_table(User, add=add)
    valid_sql = """\
    ALTER TABLE "public"."users"
        ADD COLUMN "password" CHARACTER VARYING(255) NOT NULL,
         ADD COLUMN "username" CHARACTER VARYING(255) NOT NULL UNIQUE;

    CREATE UNIQUE INDEX "users_by_username_idx" ON "public"."users" (
        lower("username"::varchar)
    );
    """

    assert sql_equals(sql, valid_sql)


def test_render_alter_table_with_multi_column_index(pg_conn, sql_equals, Employee):
    add = {
        'fields': [Employee.schema().column('first_name'), Employee.schema().column('last_name')],
        'indexes': [Employee.schema().index('by_first_and_last_name')]
    }
    sql, _ = pg_conn.render_alter_table(Employee, add=add)
    valid_sql = """\
    ALTER TABLE "public"."employees"
        ADD COLUMN "first_name" CHARACTER VARYING(255) ,
        ADD COLUMN "last_name" CHARACTER VARYING(255) ;

    CREATE INDEX "employees_by_first_and_last_name_idx" ON "public"."employees" (
        lower("first_name"::varchar), lower("last_name"::varchar)
    );
    """

    assert sql_equals(sql, valid_sql)


# create table sql

def test_render_create_table(pg_conn, sql_equals, User):
    sql, _ = pg_conn.render_create_table(User)

    valid_sql = """\
    CREATE SEQUENCE "users_id_seq";
    CREATE TABLE IF NOT EXISTS "public"."users" (
        "id" BIGINT DEFAULT nextval('users_id_seq')  NOT NULL UNIQUE PRIMARY KEY,
        "password" CHARACTER VARYING(255) NOT NULL  ,
        "username" CHARACTER VARYING(255) NOT NULL UNIQUE
        )
    WITH (OIDS=FALSE);
    """

    assert sql_equals(sql, valid_sql)


def test_render_create_table_with_standard_columns(pg_conn, sql_equals, StandardColumn):
    sql, _ = pg_conn.render_create_table(StandardColumn)

    valid_sql = """\
    CREATE SEQUENCE "standard_columns_id_seq";
    CREATE TABLE IF NOT EXISTS "testing"."standard_columns" (
        "id" BIGINT DEFAULT nextval('standard_columns_id_seq')  NOT NULL UNIQUE PRIMARY KEY,
        "binary_test" TEXT ,
        "bool_test" BOOLEAN ,
        "date_test" DATE ,
        "datetime_test" TIMESTAMP WITHOUT TIME ZONE ,
        "datetime_tz_test" TIMESTAMP WITHOUT TIME ZONE ,
        "decimal_test" DECIMAL(65, 30) ,
        "enum_column" BIGINT ,
        "float_test" DOUBLE PRECISION ,
        "integer_test" INTEGER ,
        "interval_test" INTERVAL ,
        "json_test" TEXT ,
        "long_test" BIGINT ,
        "parent_string_test_id" CHARACTER VARYING(255) REFERENCES "testing"."standard_columns" ("string_test") ,
        "parent_test_id" BIGINT REFERENCES "testing"."standard_columns" ("id") ,
        "query_test" TEXT ,
        "string_test" CHARACTER VARYING(255) ,
        "text_test" TEXT ,
        "time_test" TIME ,
        "timestamp_test" BIGINT ,
        "yaml_test" TEXT
        )
    WITH (OIDS=FALSE);
    """
    assert sql_equals(sql, valid_sql)


def test_render_create_table_with_inheritance(pg_conn, sql_equals, Employee):
    sql, _ = pg_conn.render_create_table(Employee)

    valid_sql = """\
    CREATE TABLE IF NOT EXISTS "public"."employees" (
        "first_name" CHARACTER VARYING(255) ,
        "last_name" CHARACTER VARYING(255)
        )
    INHERITS ("users")
    WITH (OIDS=FALSE);
    """

    assert sql_equals(sql, valid_sql)


def test_render_create_table_with_text(pg_conn, sql_equals, Comment):
    sql, _ = pg_conn.render_create_table(Comment)

    valid_sql = """\
    CREATE SEQUENCE "comments_id_seq";
    CREATE TABLE IF NOT EXISTS "public"."comments" (
        "id" BIGINT DEFAULT nextval('comments_id_seq')  NOT NULL UNIQUE PRIMARY KEY,
        "text" TEXT
        )
    WITH (OIDS=FALSE);
    """

    assert sql_equals(sql, valid_sql)


def test_render_create_table_with_i18n(pg_conn, sql_equals, Page):
    sql, _ = pg_conn.render_create_table(Page)

    valid_sql = """\
    CREATE SEQUENCE "pages_id_seq";
    CREATE TABLE IF NOT EXISTS "public"."pages" (
        "id" BIGINT DEFAULT nextval('pages_id_seq') NOT NULL UNIQUE  PRIMARY KEY
        )
    WITH (OIDS=FALSE);
    CREATE TABLE IF NOT EXISTS "public"."pages_i18n" (
        "locale" character varying(5),
        "pages_id" BIGINT REFERENCES "public"."pages" ("id") ON DELETE CASCADE,
        "body" TEXT ,
        "title" CHARACTER VARYING(255) ,
        CONSTRAINT "pages_i18n_pkey" PRIMARY KEY ("pages_id", "locale")
    ) WITH (OIDS=FALSE);
    """

    assert sql_equals(sql, valid_sql)


def test_render_create_table_with_increment(pg_conn, sql_equals, Incrementer):
    sql, _ = pg_conn.render_create_table(Incrementer)

    valid_sql = """\
    CREATE SEQUENCE "incrementers_id_seq";
    CREATE SEQUENCE "incrementers_count_seq";
    CREATE TABLE IF NOT EXISTS "public"."incrementers" (
        "id" BIGINT DEFAULT nextval('incrementers_id_seq')  NOT NULL UNIQUE PRIMARY KEY,
        "count" INTEGER DEFAULT nextval('incrementers_count_seq')
        )
    WITH (OIDS=FALSE);
    """

    assert sql_equals(sql, valid_sql)


# create namespace sql

def test_render_create_namespace(pg_conn, sql_equals):
    sql = pg_conn.render('create_namespace.sql.jinja', {'namespace': 'testing'})
    assert sql == 'CREATE SCHEMA IF NOT EXISTS "testing";'


# delete sql


def test_render_delete_null_collection(pg_conn):
    import orb

    sql, data = pg_conn.render_delete_collection(orb.Collection())

    assert sql == ''


def test_render_delete_all_records(pg_conn, sql_equals, Employee):
    import orb

    collection = orb.Collection(model=Employee)

    sql, data = pg_conn.render_delete_collection(collection)

    valid_sql = """\
        DELETE FROM "public".employees
        RETURNING *;
        """

    assert sql_equals(sql, valid_sql, data)


def test_render_delete_empty_collection(pg_conn, sql_equals, Employee):
    import orb

    collection = Employee.select(where=orb.Query('first_name').in_([]))

    sql, data = pg_conn.render_delete_collection(collection)

    assert sql == ''


def test_render_delete_from_loaded_collection(pg_conn, sql_equals, Employee):
    import orb

    collection = orb.Collection([Employee({'id': 1})])

    sql, data = pg_conn.render_delete_collection(collection)

    valid_sql = """\
        DELETE FROM "public".employees
        WHERE "public"."employees"."id" IN (1,)
        RETURNING *;
        """

    assert sql_equals(sql, valid_sql, data)


def test_render_delete_from_unloaded_collection(pg_conn, sql_equals, Employee):
    import orb

    collection = Employee.select(where=orb.Query('id') == 1)

    sql, data = pg_conn.render_delete_collection(collection)

    valid_sql = """\
        DELETE FROM "public".employees
        WHERE "public"."employees"."id" = 1
        RETURNING *;
        """

    assert sql_equals(sql, valid_sql, data)


def test_render_delete_from_unloaded_collection_for_all_records(pg_conn, sql_equals, Employee):
    import orb

    collection = Employee.select(where=orb.Query('id').not_in([]))

    sql, data = pg_conn.render_delete_collection(collection)

    valid_sql = """\
        DELETE FROM "public".employees
        RETURNING *;
        """

    assert sql_equals(sql, valid_sql, data)


# query sql

def test_render_query_with_and_compound(pg_conn, sql_equals, Employee):
    import orb
    a = orb.Query('first_name') == 'John'
    b = orb.Query('last_name') == 'Doe'
    sql, data = pg_conn.render_query(Employee, a & b)

    valid_sql = """\
    (
        "public"."employees"."first_name" = John AND "public"."employees"."last_name" = Doe
    )
    """

    assert sql_equals(sql, valid_sql, data)


def test_render_query_with_or_compound(pg_conn, sql_equals, Employee):
    import orb
    a = orb.Query('first_name') == 'John'
    b = orb.Query('last_name') == 'Doe'

    sql, data = pg_conn.render_query(Employee, a | b)

    valid_sql = """\
    (
        "public"."employees"."first_name" = John OR "public"."employees"."last_name" = Doe
    )
    """

    assert sql_equals(sql, valid_sql, data)


def test_render_query_with_math(pg_conn, sql_equals, Employee):
    import orb

    query = (orb.Query('first_name') + ' ' + orb.Query('last_name')) == 'John Doe'
    sql, data = pg_conn.render_query(Employee, query)

    valid_sql = '(("public"."employees"."first_name" ||  ) || "public"."employees"."last_name") = John Doe'

    assert sql_equals(sql, valid_sql, data)


def test_render_query_with_functions(pg_conn, sql_equals, Employee):
    import orb

    query = orb.Query('first_name').lower().upper() == 'JOHN'
    sql, data = pg_conn.render_query(Employee, query)

    valid_sql = 'upper(lower("public"."employees"."first_name")) = JOHN'

    assert sql_equals(sql, valid_sql, data)


def test_render_query_with_math_and_functions(pg_conn, sql_equals, Employee):
    import orb
    query = (orb.Query('first_name').lower() + ' ' + orb.Query('last_name').lower()) == 'john doe'
    sql, data = pg_conn.render_query(Employee, query)

    valid_sql = '((lower("public"."employees"."first_name") ||  ) || lower("public"."employees"."last_name")) = john doe'

    assert sql_equals(sql, valid_sql, data)

