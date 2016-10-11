"""
Defines decorators that will be used throughout the ORB library.
"""

import demandimport
import functools
import inflection
import warnings

with demandimport.enabled():
    import orb


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
                func.__orb__.set_flags(func.__orb__.flags() & ~cls.Flags.ReadOnly)
                func.__orb__.setter()(setter_func)
                return setter_func
            return setter_wrapped

        def define_query_filter():
            def shortcut_wrapped(shortcut_func):
                func.__orb__.filter(shortcut_func)
                return shortcut_func
            return shortcut_wrapped

        func.__orb__ = cls(**options)
        func.__orb__.getter()(func)
        func.setter = define_setter
        func.filter = define_query_filter
        return func
    return wrapped


def deprecated(func):
    '''This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.'''
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.warn_explicit(
            "Call to deprecated function {}.  {}".format(func.__name__, func.__doc__),
            category=DeprecationWarning,
            filename=func.func_code.co_filename,
            lineno=func.func_code.co_firstlineno + 1
        )
        return func(*args, **kwargs)
    return new_func