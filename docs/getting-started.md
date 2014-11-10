Introduction
==========================

Define a table
--------------------------

Good news, creating tables is easy.

You are just creating a Python class that inherits from the `orb.Table` object.
An `orb.TableSchema` will be associated with your model that contains all the meta database information about
your class.  On top of the table's schema, you will also be able to define additional database objects -- columns,
indexes and pipes -- on your model object.

    import orb
    
    class User(orb.Table):
        __db_columns__ = [
            orb.Column(orb.ColumnType.String, 'username')
        ]

Thats it, you now have your first ORB model!

Connecting to a database
--------------------------

Until you can really work with the system, you will need a database to store your records into.

The fastest way to get started with ORB is using the `sqlite` database type, since it is included with Python.  A
full list of available backends is found on our [databases](advanced/databases.md) page.

    >>> # create a new database instance, using the SQLite backend
    >>> db = orb.Database('SQLite', 'data.sql')
    
    >>> # activating the database -- this will assign this database instance as the active and
    >>> # default database for the models to communicate with.
    >>> db.activate()

    >>> # syncing the database -- activating the database will only open a connection to it, but you
    >>> # will need to sync your models to it before you can store them (you will only need to do this
    >>> # when your models change.
    >>> db.sync()

Records
==========================

Creating records
--------------------------

Once you have some models defined and a database to store information to, you can start storing and accessing
your records.  A `record` of a `Table` is an individual instance of one of your classes.

    >>> # creating a new user
    >>> user = User()
    
    >>> # set the username information
    >>> user.setUsername('john.doe')
    
    >>> # get the username
    >>> user.username()
    'john.doe'

A couple of things to note from the above example.  The first, the above code works on the object in memory -- it
will only submit the data to store into the database on commit.  The second thing to note is that we did not actually
define the methods we're calling on our `Table` object explicitly, instead they were auto-generated for us from
our `Column` objects.

ORB uses a getter/setter paradigm rather than a property system, which makes it easier and more transparent when
working with inherited classes and tables that need to overload the accessors.  Remember, ORB is an API builder
more than just a model definer.  How do we define the accessors then you ask?  We'll go into it in more depth later on.

Storing records
---------------------------

When you are ready to commit your record to the database, just call the `commit` method.

    >>> # before you commit, the instance will not represent a record in the database
    >>> user.isRecord()
    False
    >>> user.id()
    None

    >>> # store the record in the database
    >>> user.commit()
    True
    
    >>> # but once the record has been commited, it is now linked to the database
    >>> user.isRecord()
    True
    >>> user.id()
    1

Retrieving records
------------------------------

Accessing previously stored records from the databases can be done in a couple of ways, the most direct of
which is doing a direct lookup using a records unique id.  To do this, you simply provide the id as the 
construction argument for your instance.

    >>> # retreive all the records
    >>> users = User.all()
    >>> len(users)
    1
    >>> user = users.first()
    >>> user.id()
    1

    >>> # retreive an individual record by its id
    >>> user = User(1)
    >>> user.isRecord()
    True
    
    >>> # invalid records will raise the orb.errors.RecordNotFound error
    >>> user = User(2)
    Exception: RecordNotFound(User, 2)

It is important to note that the id that you are providing is an _argument_ and not _keywords_.  When
you provide an _argument_ to your class, it will treat it as the id for a record lookup, but when you provide
_keywords_, they are used as default values for your columns.  They will __NOT__ perform a lookup on the table.

For instance:

    >>> # this will create a new user with the username as 'john.doe', it WILL NOT lookup
    >>> # the existing record whose username is 'john.doe'
    >>> user = User(username='john.doe')
    >>> user.isRecord()
    False

If you want to lookup a record based on its username, you will need to use an `Index` instead.

Indexes
=================================

Single-Column Index
---------------------------------

An `orb.Index` is a way to retrieve a single record, or a set of records, from the database via one or
more columns.  The simplest index can actually be directly defined in the `Column` definition, provided it
only requires that one column for access.

If we modify our `User` model from the example above, we can update the `username` definition to define our
accessor:

    import orb
    
    class User(orb.Table):
        __db_columns__ = [
            orb.Column(orb.ColumnType.String, 'username', unique=True, indexed=True)
        ]

A couple of things changed here -- we're now passing in `unique=True` and `indexed=True`.  The first option
will let the database know that every record in for the User model should have a unique username.  The second
option will create an index method to lookup User's by their username.  Taken together, the result will be a 
single record from the database that can be looked up by its username (if unique is not set to True, the index will return a `RecordSet`).

    >>> # retreive a user
    >>> user = User.byUsername('john.doe')
    >>> user.id()
    1

Multiple-Column Index
---------------------------------

Sometimes, you'll need to use more than just one column when doing an indexed lookup.  To do this, you'll need
to provide some actual `orb.Index` objects to your model's definition.

    import orb
    
    class User(orb.Table):
        __db_columns__ = [
            orb.Column(orb.ColumnType.String, 'username', unique=True, indexed=True),
            orb.Column(orb.ColumnType.String, 'firstName')
            orb.Column(orb.ColumnType.String, 'lastName')
        ]
        __db_indexes__ = [
            orb.Index('byFirstAndLastName', ['firstName', 'lastName'])
        ]

For this example, we created a couple new columns -- `firstName` and `lastName`.  Here, we did not specify that
the index is `unique`, because you could have more than one person in your database named `John Doe`, so this index
will return a number of records within an `orb.RecordSet` instance.

    >>> # first, now that we have actually added new columns to our model, we will need to re-sync our database
    >>> db.sync()

    >>> # then, we will need to update our existing record
    >>> user = User.byUsername('john.doe')
    >>> user.setFirstName('John')
    >>> user.setLastName('Doe')
    >>> user.commit()
    
    >>> # finally, lets use our new index to find our existing record
    >>> users = User.byFirstAndLastName('John', 'Doe')
    >>> len(users)
    1
    >>> users[0].username()
    'john.doe'

Relationships
==========================

Foreign Key's (One-to-One/Many)
--------------------------

One of the biggest keys to relational databases is the ability to create `relationships`...hence the name.  In
ORB, it is easy to do this.  One of the nice things that ORB handles well for you is the order in which you
need to define the relationships -- you can define them in whatever way you want, it will do the lookup later.

To create a relationship, we need to first create another table to relate our User to.

    import orb
    
    class User(orb.Table):
        __db_columns__ = [
            orb.Column(orb.ColumnType.String, 'username', unique=True, indexed=True),
            orb.Column(orb.ColumnType.String, 'firstName')
            orb.Column(orb.ColumnType.String, 'lastName')
        ]
        __db_indexes__ = [
            orb.Index('byFirstAndLastName', ['firstName', 'lastName'])
        ]

    class Address(orb.Table):
        __db_columns__ = [
            # define the data columns
            orb.Column(orb.ColumnType.String, 'street'),
            orb.Column(orb.ColumnType.String, 'city'),
            orb.Column(orb.ColumnType.String, 'state'),
            orb.Column(orb.ColumnType.Integer, 'zipcode')
            
            # define the relationships
            orb.Column(orb.ColumnType.ForeignKey, 'user', reference='User')
        ]

In this example, we have created a new `Address` table and linked it to our `User` via the user `ForeignKey` column.
We define this as a `ForeignKey` and not as an `Integer` because we are going to let the database decide what
type of key is used for the id on a table (in _mongodb_ for instance, this is a string hash vs. an integer).

The real key here though is the reference keyword.  This will tell the system what table that the column
is pointing at.  In this case, we want to let the user have multiple addresses on file, so the relationship is
a _one-to-many_ in that one User can have many Addresses.  If we wanted to create this as a _one-to-one_ relationship
we would simply provide the `unique=True` keyword to the user column definition.

    >>> # first, we need to sync our new table
    >>> db.sync()
    
    >>> # retreive our john doe
    >>> user = User(1)
    
    >>> # now we can begin to create a new address
    >>> addr = Address(street='123 Main St.', city='New York', state='NY', zipcode=12345, user=user)
    >>> addr.commit()

Now that we have created an address for John, we'd like to be able to retreive it.  With the knowledge we have
already, we could create an index out of the user column on the Address table, and using that we can lookup
all Address's where the user is John.

But that is a lot of work.

Alternatively, we can tell the user column to create a reverse-lookup method.  What this will do is create a 
method from the Address's user column onto the User table.  This will make it much easier to find John's addresses.
If we modify the Address table to read:

    class Address(orb.Table):
        __db_columns__ = [
            # define the data columns
            orb.Column(orb.ColumnType.String, 'street'),
            orb.Column(orb.ColumnType.String, 'city'),
            orb.Column(orb.ColumnType.String, 'state'),
            orb.Column(orb.ColumnType.Integer, 'zipcode')
            
            # define the relationships
            orb.Column(orb.ColumnType.ForeignKey, 'user', reference='User', reversedName='addresses')
        ]

We will have told the user column of Address that we would like a reverse-lookup method on our user table, which
we can now access for John:

    >>> # get john
    >>> user = User.byUsername('john.doe')
    >>> user.addresses()
    <orb.RecordSet>
    >>> len(user.addresses())
    1

Pipes (Many-to-Many)
--------------------------

Many to many relationships can be defined using `Pipe` objects in ORB.  We decided not to provide an automated
system for this since that makes a little too much under-the-hood magic for our taste.  Instead, you will be
able to control the naming and fields that you will access and then pipe the queries through the joiner table.

For instance, if we want to set up a many-to-many relationship between a user and a group, we would set that up
as follows:

    import orb
    
    class User(orb.Table):
        __db_columns__ = [
            orb.Column(orb.ColumnType.String, 'username', unique=True, indexed=True),
            orb.Column(orb.ColumnType.String, 'firstName')
            orb.Column(orb.ColumnType.String, 'lastName')
        ]
        __db_indexes__ = [
            orb.Index('byFirstAndLastName', ['firstName', 'lastName'])
        ]
        __db_pipes__ = [
            orb.Pipe('groups', through='GroupUser', source='user', target='group')
        ]

    class Group(orb.Table):
        __db_columns__ = [
            orb.Column(orb.ColumnType.String, 'name', unique=True, indexed=True)
        ]
        __db_pipes__ = [
            orb.Pipe('users', through='GroupUser', source='group', target='user')
        ]
    
    class GroupUser(orb.Table):
        __db_columns__ = [
            orb.Column(orb.ColumnType.ForeignKey, 'user', reference='User'),
            orb.Column(orb.ColumnType.ForeignKey, 'group', reference='Group')
        ]

As you can see, you create a many-to-many relationship between tables through an intermediary table, and defining 
pipe objects on each table that you want to access the relationships through.

    >>> # create a new group
    >>> grp = Group(name='Employees')
    >>> grp.commit()
    
    >>> # grab the user's groups
    >>> user = User.byUsername('john.doe')
    >>> user.groups()
    <orb.PipeRecordSet>
    >>> len(user.groups())
    0
    
    >>> # associate the user to the group
    >>> user.groups().addRecord(grp)
    True

That just about covers the basic options when it comes to defining the database schema.  From here you will be able
to create your tables, columns and relationships for a simple database.  But there is so much more that you can do.
Sometimes, you will need to generate queries into the system taht shouldn't be defined as pipes or indexes, which
is where our [query structure](advanced/querying.md) comes in.

Debugging
===========================================

One thing that will not be obvious at this point is exactly _when_ a query gets executed on the database.  RecordSet
objects themselves do not actually query the database just yet, but exist as representations that can be further
filtered.  If you are curious about the queries that get executed and when they occur, you can enable the debug
logging in the system by doing:

    >>> import orb
    >>> orb.logger.setLevel(orb.logging.DEBUG)
