import wiji
import time
import typing
import asyncio
import logging
import datetime
import functools
import concurrent
import botocore.config
import botocore.session


from . import buffer


# See SQS limits: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-limits.html
# TODO: we need to add this limits as validations to this broker


# TODO: add batching
# TODO: add compression
# TODO: add long-polling; https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html


# TODO: rename functions and log events
import random
import string


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
        long_poll: bool = False,
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
        self.DelaySeconds = DelaySeconds

        self.QueueUrl: typing.Union[None, str] = None
        self.aws_region_name = aws_region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        self.boto_config = botocore.config.Config(
            region_name=self.aws_region_name,
            user_agent="wiji-SqsBroker",
            connect_timeout=60,
            read_timeout=60,
        )
        self.session = botocore.session.Session()
        self.client = self.session.create_client(
            service_name="sqs",
            region_name=self.aws_region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            use_ssl=True,
            config=self.boto_config,
        )
        if queue_tags is not None:
            self.queue_tags = queue_tags
        else:
            self.queue_tags = {"user": "wiji.SqsBroker"}
        if len(self.queue_tags) > 50:
            raise ValueError("AWS does not recommend setting more than 50 `queue_tags`")
        self.tags_added: bool = False

        self.task_receipt: typing.Dict[str, str] = {}
        self._thread_name_prefix: str = "wiji-SqsBroker-thread-pool"
        self.loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()

        self.recieveBuf: buffer.ReceiveBuffer = buffer.ReceiveBuffer()
        self.sendBuf: buffer.SendBuffer = buffer.SendBuffer()

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

    def _sanity_check_logger(self, event: str) -> None:
        """
        Called when we want to make sure the supplied logger can log.
        """
        try:
            self.logger.log(logging.DEBUG, {"event": event})
        except Exception as e:
            raise e

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
            await self.loop.run_in_executor(
                executor, functools.partial(self._create_queue, queue_name=queue_name)
            )
            if not self.tags_added:
                await self.loop.run_in_executor(executor, functools.partial(self._tag_queue))

    def _create_queue(self, queue_name: str) -> None:
        try:
            response = self.client.create_queue(
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
            response.update({"event": "wijisqs.SqsBroker._create_queue"})
            self.logger.log(logging.DEBUG, response)
            self.QueueUrl = response["QueueUrl"]
        except Exception as e:
            raise e

    def _tag_queue(self):
        response = self.client.tag_queue(QueueUrl=self.QueueUrl, Tags=self.queue_tags)
        response.update({"event": "wijisqs.SqsBroker._tag_queue"})
        self.logger.log(logging.DEBUG, response)
        self.tags_added = True

    async def enqueue(
        self, item: str, queue_name: str, task_options: wiji.task.TaskOptions
    ) -> None:
        """
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:

            if self.batch_send:
                buf_sife = self.sendBuf.size()
                buf_updated_at = self.sendBuf.last_update_time()
                now = time.monotonic()
                time_since_update = now - buf_updated_at
                # if we havent sent in last X seconds and there is something to send or
                # number of items in buffer is atleast 10, then send now.
                if (time_since_update > self.batching_duration and buf_sife > 0) or (
                    buf_sife >= 10
                ):
                    # take items from buffer & send batch
                    Entries = []
                    buffer_entries = self.sendBuf.give_me_ten()
                    for entry in buffer_entries:
                        thingy = {
                            "Id": entry["id"],
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
                    await self.loop.run_in_executor(
                        executor, functools.partial(self._send_message_batch, Entries=Entries)
                    )
                    # put the incoming one item into buffer
                    buffer_entry = {
                        "id": "".join(random.choices(string.ascii_uppercase + string.digits, k=17)),
                        "msg_body": item,
                        "delay": self._calculate_msg_delay(task_options=task_options),
                        "eta": task_options.eta,
                        "task_id": task_options.task_id,
                        "task_hook_metadata": task_options.hook_metadata,
                    }
                    self.sendBuf.put(new_item=buffer_entry)
                else:
                    # dont send, put in buffer
                    buffer_entry = {
                        "id": "".join(random.choices(string.ascii_uppercase + string.digits, k=17)),
                        "msg_body": item,
                        "delay": self._calculate_msg_delay(task_options=task_options),
                        "eta": task_options.eta,
                        "task_id": task_options.task_id,
                        "task_hook_metadata": task_options.hook_metadata,
                    }
                    self.sendBuf.put(new_item=buffer_entry)
            else:
                await self.loop.run_in_executor(
                    executor,
                    functools.partial(
                        self._send_message,
                        item=item,
                        queue_name=queue_name,
                        task_options=task_options,
                    ),
                )

    def _calculate_msg_delay(self, task_options: wiji.task.TaskOptions):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        task_eta = task_options.eta
        task_eta = wiji.protocol.Protocol._from_isoformat(task_eta)

        delay = self.DelaySeconds
        if task_eta > now:
            diff = task_eta - now
            delay = diff.seconds
            if delay > 900:
                delay = 900
        return delay

    def _send_message(
        self, item: str, queue_name: str, task_options: wiji.task.TaskOptions
    ) -> None:
        delay = self._calculate_msg_delay(task_options=task_options)
        response = self.client.send_message(
            QueueUrl=self.QueueUrl,
            MessageBody=item,
            # DelaySeconds is the length of time, in seconds, for which to delay a specific message
            DelaySeconds=delay,
            MessageAttributes={
                "user": {"DataType": "String", "StringValue": "wiji.SqsBroker"},
                "task_eta": {"DataType": "String", "StringValue": task_options.eta},
                "task_id": {"DataType": "String", "StringValue": task_options.task_id},
                "task_hook_metadata": {
                    "DataType": "String",
                    "StringValue": task_options.hook_metadata or "empty",
                },
            },
        )
        response.update({"event": "wijisqs.SqsBroker._send_message"})
        self.logger.log(logging.DEBUG, response)
        _ = response["MD5OfMessageBody"]
        _ = response["MD5OfMessageAttributes"]
        _ = response["MessageId"]

    def _send_message_batch(self, Entries: typing.List[typing.Dict]) -> None:
        # TODO: validate size
        # The maximum allowed individual message size and the maximum total payload size
        # (the sum of the individual lengths of all of the batched messages) are both 256 KB (262,144 bytes).
        response = self.client.send_message_batch(QueueUrl=self.QueueUrl, Entries=Entries)
        response.update({"event": "wijisqs.SqsBroker._send_message_batch"})
        self.logger.log(logging.DEBUG, response)

    async def dequeue(self, queue_name: str) -> str:
        """
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            while True:
                if self.long_poll:
                    item = self.recieveBuf.get()
                    if item:
                        return item
                    else:
                        await self.loop.run_in_executor(
                            executor,
                            functools.partial(self._receive_message_POLL, queue_name=queue_name),
                        )
                        await asyncio.sleep(1 / 117)
                else:
                    item = await self.loop.run_in_executor(
                        executor,
                        functools.partial(self._receive_message_NO_poll, queue_name=queue_name),
                    )
                    if item:
                        return item
                    else:
                        await asyncio.sleep(5)

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
        response = self.client.receive_message(
            QueueUrl=self.QueueUrl,
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
        response.update({"event": "wijisqs.SqsBroker._receive_message"})
        self.logger.log(logging.DEBUG, response)

        if self.long_poll:
            if len(response["Messages"]) >= 1:
                for msg in response["Messages"]:
                    ReceiptHandle = msg["ReceiptHandle"]
                    MessageAttributes = msg["MessageAttributes"]
                    task_id = MessageAttributes["task_id"]["StringValue"]
                    _ = MessageAttributes["task_eta"]["StringValue"]
                    _ = MessageAttributes["task_hook_metadata"]["StringValue"]
                    self.task_receipt[task_id] = ReceiptHandle
                    msg_body = msg["Body"]
                    self.recieveBuf.put(new_item=msg_body)
            return None
        else:
            if len(response["Messages"]) >= 1:
                msg = response["Messages"][0]
                ReceiptHandle = msg["ReceiptHandle"]
                MessageAttributes = msg["MessageAttributes"]
                task_id = MessageAttributes["task_id"]["StringValue"]
                _ = MessageAttributes["task_eta"]["StringValue"]
                _ = MessageAttributes["task_hook_metadata"]["StringValue"]
                self.task_receipt[task_id] = ReceiptHandle

                msg_body = msg["Body"]
                return msg_body
            else:
                return None

    async def done(
        self,
        item: str,
        queue_name: str,
        task_options: wiji.task.TaskOptions,
        state: wiji.task.TaskState,
    ) -> None:
        """
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            await self.loop.run_in_executor(
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
        task_options: wiji.task.TaskOptions,
        state: wiji.task.TaskState,
    ) -> None:
        ReceiptHandle = self.task_receipt.pop(task_options.task_id, None)
        if ReceiptHandle:
            response = self.client.delete_message(
                QueueUrl=self.QueueUrl, ReceiptHandle=ReceiptHandle
            )
            response.update({"event": "wijisqs.SqsBroker._delete_message"})
            self.logger.log(logging.DEBUG, response)
