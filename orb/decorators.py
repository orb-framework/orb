"""
Defines decorators that will be used throughout the ORB library.
"""

import inflection

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


def virtual(cls, **options):
    """
    Allows for defining virtual columns and collectors on models -- these
    are objects that are defined in code and not directly in a data store.

    :param cls:
    :param options:
    :return:
    """
    def wrapped(func):
        param_name = inflection.underscore(func.__name__)
        options.setdefault('name', param_name)

        if 'flags' in options:
            if isinstance(options['flags'], set):
                options['flags'].add('Virtual')
                options['flags'].add('ReadOnly')
            else:
                options['flags'] |= (cls.Flags.Virtual | cls.Flags.ReadOnly)
        else:
            options['flags'] = {'Virtual', 'ReadOnly'}

        def define_setter():
            def setter_wrapped(setter_func):
                func.__orb__.setFlags(func.__orb__.flags() & ~cls.Flags.ReadOnly)
                func.__orb__.setter()(setter_func)
                return setter_func
            return setter_wrapped

        def define_query_filter():
            def shortcut_wrapped(shortcut_func):
                func.__orb__.queryFilter(shortcut_func)
                return shortcut_func
            return shortcut_wrapped

        func.__orb__ = cls(**options)
        func.__orb__.getter()(func)
        func.setter = define_setter
        func.queryFilter = define_query_filter
        return func
    return wrapped