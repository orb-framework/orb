from projex.lazymodule import lazy_import

orb = lazy_import('orb')


def virtual(cls, **options):
    def wrapped(func):
        param_name = orb.system.syntax().name(func.__name__)
        options['name'] = param_name
        is_column = issubclass(cls, orb.Column)

        if 'flags' in options:
            if isinstance(options['flags'], set):
                options['flags'].add('Virtual')
                options['flags'].add('ReadOnly')
            else:
                options['flags'] &= (cls.Flags.Virtual & cls.Flags.ReadOnly)
        else:
            options['flags'] = {'Virtual', 'ReadOnly'}

        def define_setter(func):
            func.__orb__.setFlags(func.__orb__.flags() & ~cls.Flags.ReadOnly)
            if is_column:
                func.__orb__.setSetter(func.__name__)

        if is_column:
            options['getterName'] = func.__name__
        else:
            options['getter'] = func

        func.__orb__ = cls(**options)
        func.setter = define_setter
        return func
    return wrapped