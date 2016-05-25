from projex.lazymodule import lazy_import

orb = lazy_import('orb')


def virtual(cls, **options):
    def wrapped(func):
        param_name = orb.system.syntax().name(func.__name__)
        options.setdefault('name', param_name)
        is_column = issubclass(cls, orb.Column)

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

        func.__orb__ = cls(**options)
        func.__orb__.getter()(func)
        func.setter = define_setter
        return func
    return wrapped