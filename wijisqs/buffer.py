import typing


class ReceiveBuffer:
    def __init__(self) -> None:
        # The operations .append() and .pop() on a python list
        # are thread safe;
        # https://docs.python.org/3/faq/library.html#what-kinds-of-global-value-mutation-are-thread-safe
        # however the following operations on a list are not thread-safe:
        # L.append(L[-1])
        # L[i] = L[j]
        # If we want a complete thread-safe container, we can use a
        # a python queue: https://docs.python.org/3/library/queue.html
        self.pool: typing.List[str] = []

    def put(self, new_item: str) -> None:
        self.pool.append(new_item)

    def get(self) -> typing.Union[None, str]:
        try:
            item = self.pool.pop(0)
            return item
        except IndexError:
            # empty
            return None
