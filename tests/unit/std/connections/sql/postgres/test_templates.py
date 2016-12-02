# alter table sql
def test_render_alter_table(pg_conn, sql_equals, User):
    add = {
        'fields': [User.schema().column('username'), User.schema().column('password')],
        'indexes': [User.schema().index('by_username')]
    }
    sql, _ = pg_conn.render_alter_table(User, add=add)
    valid_sql = """\
    ALTER TABLE "public"."users"
        ADD COLUMN "username" CHARACTER VARYING(255) NOT NULL UNIQUE ,
        ADD COLUMN "password" CHARACTER VARYING(255) NOT NULL ;

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
        "utc_datetime_test" TIMESTAMP ,
        "utc_timestamp_test" BIGINT ,
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


# render query

def test_render_query_with_math_and_functions(pg_conn, Employee):
    import orb
    query = (orb.Query('first_name').lower() + ' ' + orb.Query('last_name').lower()) == 'john doe'
    sql, data = pg_conn.render_query(Employee, query)
    print sql
    assert sql == ''
