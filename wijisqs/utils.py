import typing
import functools


def execute_only_once(func: typing.Callable[..., typing.Any]) -> typing.Callable[..., typing.Any]:
    """
    decorator function that ensures that the decorated function
    is only ran once.

    usage:
        class Cool:
            @execute_only_once
            def my_func(self, name):
                print("name:", name)
        c = Cool()
        c.my_func(name="John")
        c.my_func(name="James")
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not wrapper.already_run:
            wrapper.already_run = True
            return func(*args, **kwargs)

    # see issue: https://github.com/python/mypy/issues/2087
    wrapper.already_run = False  # type: ignore
    return wrapper
