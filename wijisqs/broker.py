import wiji
import time
import json
import random
import typing
import asyncio
import logging
import datetime
import threading
import functools
import concurrent
import botocore.config
import botocore.session

from . import timer
from . import buffer

if typing.TYPE_CHECKING:
    import botocore.client


# See SQS limits: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-limits.html
# TODO: we need to add this limits as validations to this broker


class SqsBroker(wiji.broker.BaseBroker):
    """
    """

    def __init__(
        self,
        aws_region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        MessageRetentionPeriod: int = 345_600,
        VisibilityTimeout: int = 30,
        DelaySeconds: int = 0,
        queue_tags: typing.Union[None, typing.Dict[str, str]] = None,
        loglevel: str = "INFO",
        log_handler: typing.Union[None, wiji.logger.BaseLogger] = None,
        long_poll: bool = True,
        # using `batch_send=True` may be problematic in some cases
        batch_send: bool = False,
        batching_duration: float = 10.00,
    ) -> None:
        self.ReceiveMessageWaitTimeSeconds: int = 20
        self.MaximumMessageSize: int = 262_144
        self._validate_args(
            aws_region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            MessageRetentionPeriod=MessageRetentionPeriod,
            MaximumMessageSize=self.MaximumMessageSize,
            ReceiveMessageWaitTimeSeconds=self.ReceiveMessageWaitTimeSeconds,
            VisibilityTimeout=VisibilityTimeout,
            DelaySeconds=DelaySeconds,
            queue_tags=queue_tags,
            loglevel=loglevel,
            log_handler=log_handler,
            long_poll=long_poll,
            batch_send=batch_send,
            batching_duration=batching_duration,
        )

        self.loglevel = loglevel.upper()
        if log_handler is not None:
            self.logger = log_handler
        else:
            self.logger = wiji.logger.SimpleLogger("wiji.SqsBroker")
        self.logger.bind(level=self.loglevel, log_metadata={})
        self._sanity_check_logger(event="sqsBroker_sanity_check_logger")
        self.long_poll = long_poll
        self.batch_send = batch_send
        self.batching_duration = batching_duration

        # The length of time, in seconds, for which Amazon SQS retains a message.
        self.MessageRetentionPeriod = MessageRetentionPeriod
        # the value of `VisibilityTimeout` should be a bit longer than the
        # time it takes to execute the dequeued task.
        # otherwise, there's a possibility of your task been executed twice.
        self.VisibilityTimeout = VisibilityTimeout
        # `MaxNumberOfMessages` is the max number of messages to return.
        # SQS never returns more messages than this value(however, fewer messages might be returned).
        self.MaxNumberOfMessages = 1
        if self.long_poll:
            self.MaxNumberOfMessages = 10
        if self.MaxNumberOfMessages < 1 or self.MaxNumberOfMessages > 10:
            raise ValueError(
                "AWS does not alow less than 1 or greater than 10 `MaxNumberOfMessages`"
            )
        # The difference between `DelaySeconds` and `VisibilityTimeout` is;
        # `DelaySeconds`      -> a message is hidden when it is first added to queue
        # `VisibilityTimeout` -> a message is hidden only after it is consumed from the queue.
        self.DelaySeconds = DelaySeconds

        self.aws_region_name = aws_region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        if queue_tags is not None:
            self.queue_tags = queue_tags
        else:
            self.queue_tags = {"user": "wiji.SqsBroker"}
        if len(self.queue_tags) > 50:
            raise ValueError("AWS does not recommend setting more than 50 `queue_tags`")

        self._thread_name_prefix: str = "wiji-SqsBroker-thread-pool"

        # keeps per queue state.
        # it looks like:
        # {
        #     "queue1": { "QueueUrl": "http://queue1_url", "recieveBuf": buffer.ReceiveBuffer(), "sendBuf": buffer.SendBuffer(), "task_receipt": {}},
        #     "queue2": { "QueueUrl": "http://queue2_url", "recieveBuf": buffer.ReceiveBuffer(), "sendBuf": buffer.SendBuffer(), "task_receipt": {}},
        # }
        self._PER_QUEUE_STATE: typing.Dict[str, typing.Dict[str, typing.Any]] = {}

        # keeps per thread state.
        # it looks like:
        # {
        #     "thread1": botocore.client.SQS,
        #     "thread2": botocore.client.SQS,
        # }
        self._PER_THREAD_STATE: typing.Dict[int, "botocore.client.SQS"] = {}

        self._SHOULD_SHUT_DOWN: bool = False
        self._LOOP: typing.Union[None, asyncio.events.AbstractEventLoop] = None

    def _validate_args(
        self,
        aws_region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        MessageRetentionPeriod: int,
        MaximumMessageSize: int,
        ReceiveMessageWaitTimeSeconds: int,
        VisibilityTimeout: int,
        DelaySeconds: int,
        queue_tags: typing.Union[None, typing.Dict[str, str]],
        loglevel: str,
        log_handler: typing.Union[None, wiji.logger.BaseLogger],
        long_poll: bool,
        batch_send: bool,
        batching_duration: float,
    ) -> None:
        if not isinstance(aws_region_name, str):
            raise ValueError(
                """`aws_region_name` should be of type:: `str` You entered: {0}""".format(
                    type(aws_region_name)
                )
            )
        if not isinstance(aws_access_key_id, str):
            raise ValueError(
                """`aws_access_key_id` should be of type:: `str` You entered: {0}""".format(
                    type(aws_access_key_id)
                )
            )
        if not isinstance(aws_secret_access_key, str):
            raise ValueError(
                """`aws_secret_access_key` should be of type:: `str` You entered: {0}""".format(
                    type(aws_secret_access_key)
                )
            )

        if loglevel.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                """`loglevel` should be one of; 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'. You entered: {0}""".format(
                    loglevel
                )
            )
        if not isinstance(log_handler, (type(None), wiji.logger.BaseLogger)):
            raise ValueError(
                """`log_handler` should be of type:: `None` or `wiji.logger.BaseLogger` You entered: {0}""".format(
                    type(log_handler)
                )
            )
        if not isinstance(long_poll, bool):
            raise ValueError(
                """`long_poll` should be of type:: `bool` You entered: {0}""".format(
                    type(long_poll)
                )
            )
        if not isinstance(batch_send, bool):
            raise ValueError(
                """`batch_send` should be of type:: `bool` You entered: {0}""".format(
                    type(batch_send)
                )
            )
        if not isinstance(batching_duration, float):
            raise ValueError(
                """`batching_duration` should be of type:: `float` You entered: {0}""".format(
                    type(batching_duration)
                )
            )

        self._validate_sqs(
            MessageRetentionPeriod=MessageRetentionPeriod,
            MaximumMessageSize=MaximumMessageSize,
            ReceiveMessageWaitTimeSeconds=ReceiveMessageWaitTimeSeconds,
            VisibilityTimeout=VisibilityTimeout,
            DelaySeconds=DelaySeconds,
            queue_tags=queue_tags,
        )

    def _validate_sqs(
        self,
        MessageRetentionPeriod: int,
        MaximumMessageSize: int,
        ReceiveMessageWaitTimeSeconds: int,
        VisibilityTimeout: int,
        DelaySeconds: int,
        queue_tags: typing.Union[None, typing.Dict[str, str]],
    ):
        if not isinstance(MessageRetentionPeriod, int):
            raise ValueError(
                """`MessageRetentionPeriod` should be of type:: `int` You entered: {0}""".format(
                    type(MessageRetentionPeriod)
                )
            )
        if MessageRetentionPeriod < 60:
            raise ValueError("""`MessageRetentionPeriod` should not be less than 60 seconds""")
        elif MessageRetentionPeriod > 1_209_600:
            raise ValueError(
                """`MessageRetentionPeriod` should not be greater than 1_209_600 seconds"""
            )

        if not isinstance(MaximumMessageSize, int):
            raise ValueError(
                """`MaximumMessageSize` should be of type:: `int` You entered: {0}""".format(
                    type(MaximumMessageSize)
                )
            )
        if MaximumMessageSize < 1024:
            raise ValueError("""`MaximumMessageSize` should not be less than 1024 bytes""")
        elif MaximumMessageSize > 262_144:
            raise ValueError("""`MaximumMessageSize` should not be greater than 262_144 bytes""")

        if not isinstance(ReceiveMessageWaitTimeSeconds, int):
            raise ValueError(
                """`ReceiveMessageWaitTimeSeconds` should be of type:: `int` You entered: {0}""".format(
                    type(ReceiveMessageWaitTimeSeconds)
                )
            )
        if ReceiveMessageWaitTimeSeconds < 0:
            raise ValueError(
                """`ReceiveMessageWaitTimeSeconds` should not be less than 0 seconds"""
            )
        elif ReceiveMessageWaitTimeSeconds > 20:
            raise ValueError(
                """`ReceiveMessageWaitTimeSeconds` should not be greater than 20 seconds"""
            )

        if not isinstance(VisibilityTimeout, int):
            raise ValueError(
                """`VisibilityTimeout` should be of type:: `int` You entered: {0}""".format(
                    type(VisibilityTimeout)
                )
            )
        if VisibilityTimeout < 0:
            raise ValueError("""`VisibilityTimeout` should not be less than 0 seconds""")
        elif VisibilityTimeout > 43200:
            raise ValueError("""`VisibilityTimeout` should not be greater than 43200 seconds""")

        if not isinstance(DelaySeconds, int):
            raise ValueError(
                """`DelaySeconds` should be of type:: `int` You entered: {0}""".format(
                    type(DelaySeconds)
                )
            )
        if DelaySeconds < 0:
            raise ValueError("""`DelaySeconds` should not be less than 0 seconds""")
        elif DelaySeconds > 900:
            raise ValueError("""`DelaySeconds` should not be greater than 900 seconds""")

        if not isinstance(queue_tags, (type(None), dict)):
            raise ValueError(
                """`queue_tags` should be of type:: `None` or `dict` You entered: {0}""".format(
                    type(queue_tags)
                )
            )
        if queue_tags == {}:
            raise ValueError("""`queue_tags` should not be an empty dictionary""")
        if queue_tags:
            for k, v in queue_tags.items():
                if not isinstance(k, str):
                    raise ValueError(
                        """the keys and values of `queue_tags` should be of type `str`"""
                    )
                if not isinstance(v, str):
                    raise ValueError(
                        """the keys and values of `queue_tags` should be of type `str`"""
                    )
        if queue_tags and len(queue_tags) > 50:
            raise ValueError("""AWS does not recommend setting more than 50 `queue_tags`""")

    def _get_loop(self,):
        if self._LOOP:
            return self._LOOP

        try:
            loop: asyncio.events.AbstractEventLoop = asyncio.get_running_loop()
        except RuntimeError:
            loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        except Exception as e:
            raise e

        # cache event loop
        self._LOOP = loop
        return loop

    def _sanity_check_logger(self, event: str) -> None:
        """
        Called when we want to make sure the supplied logger can log.
        """
        try:
            self.logger.log(logging.DEBUG, {"event": event})
        except Exception as e:
            raise e

    def _sqs_client(
        self,
        aws_region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        user_agent: str = "wiji-SqsBroker",
    ) -> "botocore.client.SQS":
        """
        this is its own function so that it can be mocked.
            with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
                mock_boto_client.return_value = MockSqs()
                # add tests here
        """
        boto_config: botocore.config.Config = botocore.config.Config(
            region_name=aws_region_name,
            user_agent=user_agent,
            connect_timeout=5,
            read_timeout=self.ReceiveMessageWaitTimeSeconds + 2,
        )
        session: botocore.session.Session = botocore.session.Session()
        client: "botocore.client.SQS" = session.create_client(
            service_name="sqs",
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            use_ssl=True,
            config=boto_config,
        )
        return client

    def _get_per_thread_client(self) -> "botocore.client.SQS":
        current_thread_identity = threading.get_ident()
        if self._PER_THREAD_STATE.get(current_thread_identity):
            return self._PER_THREAD_STATE.get(current_thread_identity)

        _sqs_client: "botocore.client.SQS" = self._sqs_client(
            aws_region_name=self.aws_region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        self._PER_THREAD_STATE.update({current_thread_identity: _sqs_client})
        return _sqs_client

    def _retry_after(self, current_retries: int) -> int:
        """
        returns the number of seconds to retry after.
        retries will happen in this sequence;
        0.5min, 1min, 2min, 4min, 8min, 16min, 32min, 16min, 16min, 16min ...
        """
        if current_retries < 0:
            current_retries = 0

        jitter = random.randint(60, 180)  # 1min-3min
        if current_retries in [0, 1]:
            # if we are retrying `long_poll` msgs, we can have a situation where;
            # 1. broker.dequeue is called, recieveBuf is empty so
            # 2. we long_poll on AWS and get back messages. we buffer them in `recieveBuf`
            # 3. broker._retry_after is called because `recieveBuf` was empty
            # 4. the `VisibilityTimeout` of the msgs we buffered in `recieveBuf` is going down.
            # 5. The solution is thus: we need to retry at a pace faster than `VisibilityTimeout`
            return int(self.VisibilityTimeout / 5)
        elif current_retries == 2:
            return int(self.VisibilityTimeout / 3)
        elif current_retries >= 6:
            return (16 * 60) + jitter  # 16 minutes + jitter
        else:
            return (60 * (2 ** current_retries)) + jitter

    def _get_per_queue_url(self, queue_name: str) -> str:
        return self._PER_QUEUE_STATE[queue_name]["QueueUrl"]

    def _set_per_queue_state(self, queue_name: str, QueueUrl: str) -> None:
        if (
            self._PER_QUEUE_STATE.get(queue_name)
            and self._PER_QUEUE_STATE[queue_name].get("recieveBuf")
            and self._PER_QUEUE_STATE[queue_name].get("sendBuf")
            and self._PER_QUEUE_STATE[queue_name].get("task_receipt")
        ):
            # this already exist and we do not want to overrite them
            return
        self._PER_QUEUE_STATE[queue_name] = {
            "QueueUrl": QueueUrl,
            "recieveBuf": buffer.ReceiveBuffer(),
            "sendBuf": buffer.SendBuffer(),
            "task_receipt": {},  # task_id_and_receipt_handle
        }

    def _get_per_queue_sendBuf(self, queue_name: str) -> buffer.SendBuffer:
        return self._PER_QUEUE_STATE[queue_name]["sendBuf"]

    def _get_per_queue_recieveBuf(self, queue_name: str) -> buffer.ReceiveBuffer:
        return self._PER_QUEUE_STATE[queue_name]["recieveBuf"]

    def _get_per_queue_task_receipt(self, queue_name: str) -> typing.Dict[str, str]:
        return self._PER_QUEUE_STATE[queue_name]["task_receipt"]

    async def check(self, queue_name: str) -> None:
        """
        - If you provide the name of an existing queue along with the exact names and values of all the queue's attributes,
          CreateQueue returns the queue URL for the existing queue.
        - If the queue name, attribute names, or attribute values don't match an existing queue, CreateQueue returns an error.
        - A queue name can have up to 80 characters.
          Valid values: alphanumeric characters, hyphens (- ), and underscores (_ ).
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            await self._get_loop().run_in_executor(
                executor, functools.partial(self._create_queue, queue_name=queue_name)
            )
            # this should always run during `check` call
            # and should run immediatley after `_create_queue` but before `_tag_queue`
            await self._get_loop().run_in_executor(
                executor, functools.partial(self._get_queue_url, queue_name=queue_name)
            )
            await self._get_loop().run_in_executor(
                executor, functools.partial(self._tag_queue, queue_name=queue_name)
            )

    def _create_queue(self, queue_name: str) -> None:
        """
        this needs to run for as many queue_name's as will be handled by this broker
        """

        if self._PER_QUEUE_STATE.get(queue_name) and self._PER_QUEUE_STATE[queue_name].get(
            "QueueUrl"
        ):
            # already exists
            return
        try:
            response = self._get_per_thread_client().create_queue(
                QueueName=queue_name,
                Attributes={
                    # MessageRetentionPeriod is the length of time, in seconds, for which Amazon SQS retains a message.
                    "MessageRetentionPeriod": str(self.MessageRetentionPeriod),
                    "MaximumMessageSize": str(self.MaximumMessageSize),
                    "ReceiveMessageWaitTimeSeconds": str(self.ReceiveMessageWaitTimeSeconds),
                    "VisibilityTimeout": str(self.VisibilityTimeout),
                    # DelaySeconds is the length of time, in seconds, for which the delivery of all messages in the queue is delayed.
                    "DelaySeconds": str(self.DelaySeconds),
                },
            )
            response.update({"event": "wijisqs.SqsBroker._create_queue", "queue_name": queue_name})
            self.logger.log(logging.DEBUG, response)
            self._set_per_queue_state(queue_name=queue_name, QueueUrl=response["QueueUrl"])
        except Exception as e:
            raise e

    def _tag_queue(self, queue_name: str) -> None:
        """
        this needs to run for as many queue_name's as will be handled by this broker
        """
        response = self._get_per_thread_client().tag_queue(
            QueueUrl=self._get_per_queue_url(queue_name=queue_name), Tags=self.queue_tags
        )
        response.update({"event": "wijisqs.SqsBroker._tag_queue", "queue_name": queue_name})
        self.logger.log(logging.DEBUG, response)

    def _get_queue_url(self, queue_name: str) -> None:
        """
        this should always run during `check` call
        it populates the queue_name and QueueUrl mapping
        """
        if self._PER_QUEUE_STATE.get(queue_name) and self._PER_QUEUE_STATE[queue_name].get(
            "QueueUrl"
        ):
            # already exists
            return
        try:
            response = self._get_per_thread_client().get_queue_url(QueueName=queue_name)
            response.update({"event": "wijisqs.SqsBroker._get_queue_url", "queue_name": queue_name})
            self.logger.log(logging.DEBUG, response)
            self._set_per_queue_state(queue_name=queue_name, QueueUrl=response["QueueUrl"])
        except Exception as e:
            raise e

    def _drain_sendBuf(self, queue_name: str) -> None:
        """
        gets called by `SqsBroker.deque` so as to drain the `sendBuf` of the particular queue.
        It is called be `.deque` because that method is always called by `wiji.Worker` in a loop(always).

        We are creating a thread per queue_name per invocation of `Task.delay`; this is very bad
        but we somehow offset that cost by checking if a thread with given name already exists and we return.
        """
        if self._SHOULD_SHUT_DOWN:
            return
        if not self.batch_send:
            return

        # take items from buffer & send batch
        sendBuf = self._get_per_queue_sendBuf(queue_name=queue_name)
        if sendBuf.size() <= 0:
            return

        buf_updated_at = sendBuf.last_update_time()
        now = time.monotonic()
        time_since_update = now - buf_updated_at
        if time_since_update < self.batching_duration:
            # TODO: we have potential of creating runaway threads
            # (threads inside other threads forever)
            timer.WijiSqsTimer(
                # we want this thread to run sooner
                interval=self.batching_duration / 2.5,
                function=self._drain_sendBuf,
                args=(queue_name,),
                name="WijiSqsTimer-{0}".format(queue_name),
            ).start()
            # if we HAVE sent in last X seconds return
            return

        while sendBuf.size() > 0:
            # while there are still any messages in the buffer,
            # send them in batches of 10.
            Entries = []
            buffer_entries = sendBuf.give_me_ten()
            for entry in buffer_entries:
                thingy = {
                    "Id": entry["task_id"],
                    "MessageBody": entry["msg_body"],
                    "DelaySeconds": entry["delay"],
                    "MessageAttributes": {
                        "user": {"DataType": "String", "StringValue": "wiji.SqsBroker"},
                        "task_eta": {"DataType": "String", "StringValue": entry["eta"]},
                        "task_id": {"DataType": "String", "StringValue": entry["task_id"]},
                        "task_hook_metadata": {
                            "DataType": "String",
                            "StringValue": entry.get("task_hook_metadata") or "empty",
                        },
                    },
                }
                Entries.append(thingy)
            self._send_message_batch(Entries=Entries, queue_name=queue_name)

    async def enqueue(self, queue_name: str, item: str) -> None:
        """
        """
        if self._SHOULD_SHUT_DOWN:
            self.logger.log(
                logging.INFO,
                {
                    "event": "wijisqs.SqsBroker.enqueue",
                    "stage": "end",
                    "state": "cleanly shutting down broker.",
                },
            )
            return None

        if self.batch_send:
            # this will create a thread per queue_name per invocation of `Task.delay`; this is very bad
            # but we somehow offset that cost by checking if a thread with given name already exists
            thread_name = "WijiSqsTimer-{0}".format(queue_name)
            if thread_name not in [i.name for i in threading.enumerate()]:
                t = timer.WijiSqsTimer(
                    interval=self.batching_duration * 1.5,
                    function=self._drain_sendBuf,
                    args=(queue_name,),
                    name=thread_name,
                )
                t.start()

        task_options = json.loads(item)["task_options"]
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            sendBuf = self._get_per_queue_sendBuf(queue_name=queue_name)
            if self.batch_send:
                buf_sife = sendBuf.size()
                buf_updated_at = sendBuf.last_update_time()
                now = time.monotonic()
                time_since_update = now - buf_updated_at
                # if we havent sent in last X seconds and there is something to send or
                # number of items in buffer is atleast 10, then send now.
                if (time_since_update > self.batching_duration and buf_sife > 0) or (
                    buf_sife >= 10
                ):
                    # take items from buffer & send batch
                    Entries = []
                    buffer_entries = sendBuf.give_me_ten()
                    for entry in buffer_entries:
                        thingy = {
                            "Id": entry["task_id"],
                            "MessageBody": entry["msg_body"],
                            "DelaySeconds": entry["delay"],
                            "MessageAttributes": {
                                "user": {"DataType": "String", "StringValue": "wiji.SqsBroker"},
                                "task_eta": {"DataType": "String", "StringValue": entry["eta"]},
                                "task_id": {"DataType": "String", "StringValue": entry["task_id"]},
                                "task_hook_metadata": {
                                    "DataType": "String",
                                    "StringValue": entry.get("task_hook_metadata") or "empty",
                                },
                            },
                        }
                        Entries.append(thingy)
                    await self._get_loop().run_in_executor(
                        executor,
                        functools.partial(
                            self._send_message_batch, Entries=Entries, queue_name=queue_name
                        ),
                    )
                    # put the incoming one item into buffer
                    buffer_entry = {
                        "task_id": task_options["task_id"],
                        "msg_body": item,
                        "delay": self._calculate_msg_delay(task_options=task_options),
                        "eta": task_options["eta"],
                        "task_hook_metadata": task_options["hook_metadata"],
                    }
                    sendBuf.put(new_item=buffer_entry)
                else:
                    # dont send, put in buffer
                    buffer_entry = {
                        "task_id": task_options["task_id"],
                        "msg_body": item,
                        "delay": self._calculate_msg_delay(task_options=task_options),
                        "eta": task_options["eta"],
                        "task_hook_metadata": task_options["hook_metadata"],
                    }
                    sendBuf.put(new_item=buffer_entry)
            else:
                await self._get_loop().run_in_executor(
                    executor,
                    functools.partial(
                        self._send_message,
                        item=item,
                        queue_name=queue_name,
                        task_options=task_options,
                    ),
                )

    def _calculate_msg_delay(self, task_options: typing.Dict[str, typing.Any]):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        task_eta = task_options["eta"]
        task_eta = wiji.protocol.Protocol._from_isoformat(task_eta)

        delay = self.DelaySeconds
        if task_eta > now:
            diff = task_eta - now
            delay = diff.seconds
            if delay > 900:
                delay = 900
        return delay

    def _send_message(
        self, item: str, queue_name: str, task_options: typing.Dict[str, typing.Any]
    ) -> None:
        delay = self._calculate_msg_delay(task_options=task_options)
        response = self._get_per_thread_client().send_message(
            QueueUrl=self._get_per_queue_url(queue_name=queue_name),
            MessageBody=item,
            # DelaySeconds is the length of time, in seconds, for which to delay a specific message
            DelaySeconds=delay,
            MessageAttributes={
                "user": {"DataType": "String", "StringValue": "wiji.SqsBroker"},
                "task_eta": {"DataType": "String", "StringValue": task_options["eta"]},
                "task_id": {"DataType": "String", "StringValue": task_options["task_id"]},
                "task_hook_metadata": {
                    "DataType": "String",
                    "StringValue": task_options.get("hook_metadata") or "empty",
                },
            },
        )
        response.update({"event": "wijisqs.SqsBroker._send_message", "queue_name": queue_name})
        self.logger.log(logging.DEBUG, response)
        _ = response["MD5OfMessageBody"]
        _ = response["MD5OfMessageAttributes"]
        _ = response["MessageId"]

    def _send_message_batch(self, Entries: typing.List[typing.Dict], queue_name: str) -> None:
        # TODO: validate size
        # The maximum allowed individual message size and the maximum total payload size
        # (the sum of the individual lengths of all of the batched messages) are both 256 KB (262,144 bytes).
        response = self._get_per_thread_client().send_message_batch(
            QueueUrl=self._get_per_queue_url(queue_name=queue_name), Entries=Entries
        )
        response.update(
            {"event": "wijisqs.SqsBroker._send_message_batch", "queue_name": queue_name}
        )
        self.logger.log(logging.DEBUG, response)

    async def dequeue(self, queue_name: str, TESTING: bool = False) -> str:
        """
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            retry_count: int = 0
            while True:
                if self._SHOULD_SHUT_DOWN:
                    self.logger.log(
                        logging.INFO,
                        {
                            "event": "wijisqs.SqsBroker.dequeue",
                            "stage": "end",
                            "state": "cleanly shutting down broker.",
                        },
                    )
                    await asyncio.sleep(self.batching_duration)
                    continue  # VERY IMPORTANT

                if self.long_poll:
                    item = self._get_per_queue_recieveBuf(queue_name=queue_name).get()
                    if item:
                        retry_count = 0
                        return item
                    else:
                        await self._get_loop().run_in_executor(
                            executor,
                            functools.partial(self._receive_message_POLL, queue_name=queue_name),
                        )
                        # we have just called AWS, so maybe we got some messages and populated the `recieveBuf`
                        # if that is the case, we should hand those msgs over to `wiji` right away instead of sleeping
                        item = self._get_per_queue_recieveBuf(queue_name=queue_name).get()
                        if item:
                            retry_count = 0
                            return item

                        # we should sleep only if the above call to AWS didn't yield any messages.
                        interval = self._retry_after(retry_count)
                        retry_count += 1
                        self.logger.log(
                            logging.INFO,
                            {
                                "event": "wijisqs.SqsBroker.dequeue",
                                "stage": "end",
                                "queue_name": queue_name,
                                "state": "queue is empty. sleeping for {0} seconds".format(
                                    interval
                                ),
                                "retry_count": retry_count,
                            },
                        )
                        await asyncio.sleep(interval)
                        if TESTING:
                            return '{"key": "mock_item"}'
                else:
                    item = await self._get_loop().run_in_executor(
                        executor,
                        functools.partial(self._receive_message_NO_poll, queue_name=queue_name),
                    )
                    if item:
                        retry_count = 0
                        return item
                    else:
                        interval = self._retry_after(retry_count)
                        retry_count += 1
                        self.logger.log(
                            logging.INFO,
                            {
                                "event": "wijisqs.SqsBroker.dequeue",
                                "stage": "end",
                                "queue_name": queue_name,
                                "state": "queue is empty. sleeping for {0} seconds".format(
                                    interval
                                ),
                                "retry_count": retry_count,
                            },
                        )
                        await asyncio.sleep(interval)
                        if TESTING:
                            return '{"key": "mock_item"}'

    def _receive_message_NO_poll(self, queue_name: str) -> typing.Union[None, str]:
        return self._receive_message(queue_name=queue_name)

    def _receive_message_POLL(self, queue_name: str) -> typing.Union[None, str]:
        return self._receive_message(queue_name=queue_name)

    def _receive_message(self, queue_name: str) -> typing.Union[None, str]:
        """
        Retrieves one or more messages (up to 10), from the specified queue.
        Using the WaitTimeSeconds parameter enables long-poll support.

        1. https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
        """
        response = self._get_per_thread_client().receive_message(
            QueueUrl=self._get_per_queue_url(queue_name=queue_name),
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=self.MaxNumberOfMessages,
            # VisibilityTimeout: The duration (in seconds) that the received messages are hidden
            # from subsequent retrieve requests after being retrieved by a ReceiveMessage request.
            # this value should be a bit longer than the time it takes to execute the dequeued task.
            # otherwise, there's a possibility of your task been executed twice.
            VisibilityTimeout=self.VisibilityTimeout,
            # WaitTimeSeconds: The duration (in seconds) for which the call waits for a message to arrive in the queue before returning.
            # If no messages are available and the wait time expires, the call returns successfully with an empty list of messages.
            WaitTimeSeconds=self.ReceiveMessageWaitTimeSeconds,
        )
        response.update({"event": "wijisqs.SqsBroker._receive_message", "queue_name": queue_name})
        self.logger.log(logging.DEBUG, response)

        if not response.get("Messages"):
            # empty response from SQS
            return None

        if self.long_poll:
            if len(response["Messages"]) >= 1:
                for msg in response["Messages"]:
                    ReceiptHandle = msg["ReceiptHandle"]
                    MessageAttributes = msg["MessageAttributes"]
                    task_id = MessageAttributes["task_id"]["StringValue"]
                    _ = MessageAttributes["task_eta"]["StringValue"]
                    _ = MessageAttributes["task_hook_metadata"]["StringValue"]
                    self._get_per_queue_task_receipt(queue_name=queue_name)[task_id] = ReceiptHandle
                    self._get_per_queue_recieveBuf(queue_name=queue_name).put(new_item=msg["Body"])
            return None
        else:
            if len(response["Messages"]) >= 1:
                msg = response["Messages"][0]
                ReceiptHandle = msg["ReceiptHandle"]
                MessageAttributes = msg["MessageAttributes"]
                task_id = MessageAttributes["task_id"]["StringValue"]
                _ = MessageAttributes["task_eta"]["StringValue"]
                _ = MessageAttributes["task_hook_metadata"]["StringValue"]
                self._get_per_queue_task_receipt(queue_name=queue_name)[task_id] = ReceiptHandle
                return msg["Body"]
            else:
                return None

    async def done(self, item: str, queue_name: str, state: wiji.task.TaskState) -> None:
        """
        """
        task_options = json.loads(item)["task_options"]
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            await self._get_loop().run_in_executor(
                executor,
                functools.partial(
                    self._delete_message,
                    item=item,
                    queue_name=queue_name,
                    task_options=task_options,
                    state=state,
                ),
            )

    def _delete_message(
        self,
        item: str,
        queue_name: str,
        task_options: typing.Dict[str, typing.Any],
        state: wiji.task.TaskState,
    ) -> None:
        ReceiptHandle = self._get_per_queue_task_receipt(queue_name=queue_name).pop(
            task_options["task_id"], None
        )
        if ReceiptHandle:
            response = self._get_per_thread_client().delete_message(
                QueueUrl=self._get_per_queue_url(queue_name=queue_name), ReceiptHandle=ReceiptHandle
            )
            response.update(
                {"event": "wijisqs.SqsBroker._delete_message", "queue_name": queue_name}
            )
            self.logger.log(logging.DEBUG, response)

    async def shutdown(self, queue_name: str, duration: float) -> None:
        """
        when this method is called:
          1. wijisqs should halt all consumption from AWS sqs
          2. it should halt any NEW publishing to sqs
          3. for any messages that are in the `ReceiveBuffer`(received from SQS but yet to be processed);
             - we do nothing since when we dequed the msg, we had set a `VisibilityTimeout` which will eventually expire and the
               message will become available for dequeuing and execution by another consumer.
          4. for any messages that are in the `SendBuffer`(yet to be sent to SQS);
             - we should send these ones out immediatley.

        All this things need to happen in parallel.
        """
        # 1. wijisqs should halt all consumption from AWS sqs
        # 2. it should halt any NEW publishing to sqs
        self._SHOULD_SHUT_DOWN: bool = True

        # 3. for any messages that are in the `ReceiveBuffer`(received from SQS but yet to be processed);
        # we do nothing, `VisibilityTimeout` will solve that problem for us.

        # 4. for any messages that are in the `SendBuffer`(yet to be sent to SQS);
        # - we should send these ones out immediatley.
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:

            # take items from buffer & send batch
            sendBuf = self._get_per_queue_sendBuf(queue_name=queue_name)
            while sendBuf.size() > 0:
                # while there are still any messages in the buffer,
                # send them in batches of 10.
                Entries = []
                buffer_entries = sendBuf.give_me_ten()
                for entry in buffer_entries:
                    thingy = {
                        "Id": entry["task_id"],
                        "MessageBody": entry["msg_body"],
                        "DelaySeconds": entry["delay"],
                        "MessageAttributes": {
                            "user": {"DataType": "String", "StringValue": "wiji.SqsBroker"},
                            "task_eta": {"DataType": "String", "StringValue": entry["eta"]},
                            "task_id": {"DataType": "String", "StringValue": entry["task_id"]},
                            "task_hook_metadata": {
                                "DataType": "String",
                                "StringValue": entry.get("task_hook_metadata") or "empty",
                            },
                        },
                    }
                    Entries.append(thingy)

                await self._get_loop().run_in_executor(
                    executor,
                    functools.partial(
                        self._send_message_batch, Entries=Entries, queue_name=queue_name
                    ),
                )
