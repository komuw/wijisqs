import time
import typing
import collections

# TODO: test this buffers

# THREAD SAFETY
# We are using a python collections.deque as our buffer
# Deques support thread-safe, memory efficient appends and pops from either side of the deque
# All this but still with approximately the same O(1) performance in either direction.
# This is opposed to python list that, incurs O(n) memory movement costs for pop(0)


class ReceiveBuffer:
    """
    Buffer to store tasks that we have received from SQS.
    """

    def __init__(self) -> None:
        self.pool: collections.deque = collections.deque(maxlen=None)

    def size(self) -> int:
        return len(self.pool)

    def put(self, new_item: str) -> None:
        self.pool.append(new_item)

    def get(self) -> typing.Union[None, str]:
        try:
            item = self.pool.popleft()
            return item
        except IndexError:
            # empty
            return None


class SendBuffer:
    """
    Buffer to store tasks that we want to send to SQS.
    """

    def __init__(self) -> None:
        self.pool: collections.deque = collections.deque(maxlen=None)
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
                items.append(self.pool.popleft())
        except IndexError:
            pass
        assert (
            len(items) <= 10
        ), "AWS does not allow a send_message_batch request to have >10 messages"
        return items
