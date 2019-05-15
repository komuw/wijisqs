import typing
import threading


class WijiSqsTimer(threading.Thread):
    """
    Call a function after a specified number of seconds:
        t = Timer(30.0, f, args=None, kwargs=None)
        t.start()
        t.cancel()     # stop the timer's action if it's still waiting

    This class is similar to the one in python stdlib; `threading.Timer`
    except this one takes an extra `name` argument in its `init` method
    https://github.com/python/cpython/blob/c923c3449f825021b13521b2380e67ba35a36f55/Lib/threading.py#L1159-L1184
    """

    def __init__(
        self,
        interval: float,
        function: typing.Callable[..., typing.Any],
        args: typing.Union[None, typing.Tuple] = None,
        kwargs: typing.Union[None, typing.Dict] = None,
        name: str = "WijiSqsTimer",
    ) -> None:
        threading.Thread.__init__(self, name=name)
        self.interval = interval
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.finished = threading.Event()

    def cancel(self) -> None:
        """Stop the timer if it hasn't finished yet."""
        self.finished.set()

    def run(self) -> None:
        self.finished.wait(self.interval)
        if not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
        self.finished.set()
