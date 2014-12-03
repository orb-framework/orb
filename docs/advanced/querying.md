Querying
==========================

Introduction
--------------------------

As we have shown in the [Getting Started](../getting-started.md) section, you can access existing database records via pipes and
indexes, but often times you will need to lookup records in a more flexible manner.  That is where our Query
objects come in.

The `orb.Query` object is a Pythonic representation of a database lookup, allowing you to create complicated
lookups in a familiar logical manner as other objects.

To get started, you will need to import the `Query` object, which is most conveniently done as:

    from orb import Query as Q

The easiest way to explain its usage is to create a couple of our examples from the previous page.  This will
show a bit about what is going on under the hood, represented using the query system.

    >>> # build a query
    >>> q = Q('username') == 'john.doe'
    
    >>> # find the first record with this query
    >>> user = User.select(where=q).first()
    >>> user.id()
    1

The first line of this example is creating the Query instance.  The first part, the `Q('username')` is what is
actually generating the query object, the second part `== 'john.doe'` is taking advantage of Python's object
operators -- so all of the standard operators can be performed on a query object.

The second part of the example is the usage of the query object.  All Table objects have access to a number
of class method accessors, which allow for selection and filtering of existing records.  The `select` method
will return a `orb.RecordSet`. 

Compounds
--------------------------

More advanced queries are created by joining together multiple query objects into a query compound.  This is done
using Python's AND and OR operators.

For instance, if we wanted to recreate the first and last name index from the previous page, we would need a compound.

    >>> # build a query compound
    >>> q = (Q('firstName') == 'John') & (Q('lastName') == 'Doe')
    
    >>> # find the first record with this query
    >>> user = User.select(where=q).first()
    >>> user.id()
    1

Build Shortcut
--------------------------

While the querying system provides the bulk of the power and flexibility of the ORB framework, it can be a bit
cumbersome for quick and easy lookups.  To address that issue, we added the build shortcut into the query.  Using
the build method will take a dictionary of key/value pairings and generate a query compound using the `==` operator.

So, the above example can be re-written with the build utility as:

    >>> # build a query compound
    >>> q = Q.build({'firstName': 'John', 'lastName': 'Doe'})

Operators
==========================

Comparisons
--------------------------

This is just a quick run down of comparative operators available within the framework.

    >>> Q('firstName') == 'John'                    # equal to
    >>> Q('fistName') != 'Bill'                     # not equal to
    >>> Q('firstName') == None                      # is null
    >>> Q('lastName') != None                       # is not null
    >>> Q('lastName').in_(['Doe', 'Deere'])         # within a list of values
    >>> Q('lastName').notIn(['Lennon'])             # not within a list of values
    
    >>> Q('created') < datetime.date.today()        # all comparators are available (>, >=, <, <=)
    >>> Q('zipcode') <= 12345
    >>> Q('created').between(datetime.date(2012, 1, 1), datetime.date(2013, 1, 1))
    >>> Q('zipcode').between(1234, 1236)            # between 2 values

String Operations
--------------------------

    >>> Q('lastName').contains('oe')
    >>> Q('lastName').doesNotContain('non')
    >>> Q('lastName').startswith('D')
    >>> Q('lastName').doesNotStartwith('L')
    >>> Q('lastName').endswith('e')
    >>> Q('lastName').doesNotEndwith('n')
    >>> Q('lastName').matches('^D.*e$')
    >>> Q('lastName').doesNotMatch('^L.*n$')

Math Operations
---------------------------

On top of the basic operations that are available, you can also perform mathematical operations
on your query column.

Some examples would be:

    >>> (Q('earned') - Q('spent')) > 0
    >>> (Q('percent') * 100) < 50
    >>> (Q('flags') & Flags.Enabled) != 0

The supported operations are standard mathematical ones (`*`, `+`, `-`, `/`, `&` and `|`).

Casting Functions
---------------------------

There are also casting functions that will be able to convert the query value types to other
values for comparitive purposes.

Here are the currently available functions:

    >>> abs(Q('amount')) > 1000
    >>> Q('zipcode').asString().contains('24')
    >>> Q('username').lower() == 'john.doe'
    >>> Q('username').upper() == 'JOHN.DOE'

Order of Operation
---------------------------

As with all math operations, when you start joining these queries together, the order of operation
is important.  You can group your queries together, and depending how you order everything, it will
change the way the query is calculated.

    >>> # create a couple example queries
    >>> a = Q('firstName') == 'John'
    >>> b = Q('lastName') == 'Doe'
    >>> c = Q('lastName') == 'Deere'
    
    >>> # creating a compound like
    >>> a & (b | c)
    firstName is John and (lastName is Doe or lastName is Deere)
    
    >>> # is very different than
    >>> (a & b) | c
    (firstName is John and lastName is Doe) or lastName is Deere