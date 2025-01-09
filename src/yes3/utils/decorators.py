from datetime import datetime
from functools import wraps
from inspect import signature


def timeit_opt(func=None, *, default=False):
    assert func is None or callable(func), 'timeit_opt parameters must be passed as keyword arguments'

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if 'timeit' in signature(f).parameters:
                timeit = kwargs.get('timeit', default)
            else:
                timeit = kwargs.pop('timeit', default)
            start = datetime.now()
            result = f(*args, **kwargs)
            if timeit:
                print(f'{f.__name__} elapsed time: {datetime.now() - start}')
            return result
        return wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)
