import time
import typing


# TODO: test this buffers

# THREAD SAFETY
# We are using a python list as our buffers.
# Most operations on lists are thread safe[1], specifically .append() and .pop()
# So we are okay unless we start doing crazy stuff like:
# L=[]; L.append(L[-1])
#
# ref:
# 1. https://docs.python.org/3/faq/library.html#what-kinds-of-global-value-mutation-are-thread-safe
# 2. issues/9


class ReceiveBuffer:
    """
    Buffer to store tasks that we have received from SQS.
    """

    def __init__(self) -> None:
        self.pool: typing.List[str] = []

    def size(self) -> int:
        return len(self.pool)

    def put(self, new_item: str) -> None:
        self.pool.append(new_item)

    def get(self) -> typing.Union[None, str]:
        try:
            item = self.pool.pop(0)
            return item
        except IndexError:
            # empty
            return None


class SendBuffer:
    """
    Buffer to store tasks that we want to send to SQS.
    """

    def __init__(self) -> None:
        self.pool: typing.List[typing.Dict] = []
        self.updated_at: float = time.monotonic()

    def size(self) -> int:
        return len(self.pool)

    def last_update_time(self) -> float:
        return self.updated_at

    def put(self, new_item: typing.Dict) -> None:
        now = time.monotonic()
        self.updated_at = now
        self.pool.append(new_item)

    def give_me_ten(self) -> typing.List[typing.Dict]:
        items = []
        try:
            for _ in range(1, 11):
                items.append(self.pool.pop(0))
        except IndexError:
            pass
        assert (
            len(items) <= 10
        ), "AWS does not allow a send_message_batch request to have >10 messages"
        return items
