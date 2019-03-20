import time
import json
import asyncio
from unittest import TestCase, mock

import wiji
import wijisqs


def AsyncMock(*args, **kwargs):
    """
    see: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


class MockSqs:
    """
    Mock various responses from AWS SQS.

    The various responses have been gotten from:
    https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    """

    def __init__(self, proto=None, mock_msg_to_receive=None) -> None:
        if proto is not None:
            self.proto = proto
        else:
            self.proto = wiji.protocol.Protocol(
                version=1,
                task_id="task_id",
                eta="2019-03-12T14:21:27.751149+00:00",
                current_retries=0,
                max_retries=0,
                log_id="log_id",
                hook_metadata="hook_metadata",
                argsy=(),
                kwargsy={"mock_protocol_kwarg": "mock_protocol_kwarg"},
            )

        self.mock_msg_to_receive = mock_msg_to_receive

    def create_queue(self, *args, **kwargs):
        return {"QueueUrl": "http://mock_SQS_QueueUrl"}

    def tag_queue(self, *args, **kwargs):
        return {}

    def get_queue_url(self, *args, **kwargs):
        return {"QueueUrl": "http://mock_SQS_QueueUrl"}

    def send_message(self, *args, **kwargs):
        return {
            "MD5OfMessageBody": "MOCK_MD5OfMessageBody",
            "MD5OfMessageAttributes": "MOCK_MD5OfMessageAttributes",
            "MessageId": "MOCK_MessageId",
        }

    def send_message_batch(self, *args, **kwargs):
        return {
            "Successful": [
                {
                    "Id": "MOCK_Id",
                    "MessageId": "MOCK_MessageId",
                    "MD5OfMessageBody": "MOCK_MD5OfMessageBody",
                    "MD5OfMessageAttributes": "MOCK_MD5OfMessageAttributes",
                    "SequenceNumber": "MOCK_SequenceNumber",
                }
            ],
            "Failed": [
                {
                    "Id": "MOCK_Id",
                    "SenderFault": True,
                    "Code": "MOCK_COde",
                    "Message": "MOCK_Message",
                }
            ],
        }

    def receive_message(self, *args, **kwargs):
        if self.mock_msg_to_receive:
            return self.mock_msg_to_receive
        else:
            MaxNumberOfMessages = kwargs["MaxNumberOfMessages"]
            msgs = []
            # account for polling
            for i in range(0, MaxNumberOfMessages):
                msg = {
                    "MessageId": "MOCK_MessageId-{0}".format(i),
                    "ReceiptHandle": "MOCK_ReceiptHandle-{0}".format(i),
                    "MD5OfBody": "MOCK_MD5OfBody-{0}".format(i),
                    "Body": self.proto.json(),
                    "Attributes": {
                        "MOCK_Attributes_KEY-{0}".format(i): "MOCK_Attributes_Value-{0}".format(i)
                    },
                    "MD5OfMessageAttributes": "MOCK_MD5OfMessageAttributes-{0}".format(i),
                    "MessageAttributes": {
                        "task_id": {
                            "StringValue": "MOCK_task_id-{0}".format(i),
                            "DataType": "string",
                        },
                        "task_eta": {
                            "StringValue": "MOCK_task_eta-{0}".format(i),
                            "DataType": "string",
                        },
                        "task_hook_metadata": {
                            "StringValue": "MOCK_task_hook_metadata-{0}".format(i),
                            "DataType": "string",
                        },
                    },
                }
                msgs.append(msg)

            return {"Messages": msgs}

    def delete_message(self, *args, **kwargs):
        return {}


class TestBroker(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_broker.TestBroker.test_something
    """

    def setUp(self):
        self.queue_name = "WijiSqsTestQueue"

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_bad_instantiation(self):
        def create_broker():
            wijisqs.SqsBroker(
                aws_region_name="aws_region_name",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key=12_331_414,
            )

        self.assertRaises(ValueError, create_broker)
        with self.assertRaises(ValueError) as raised_exception:
            create_broker()
        self.assertIn(
            "`aws_secret_access_key` should be of type:: `str` You entered: <class 'int'>",
            str(raised_exception.exception),
        )

    def test_fulfills_wiji_broker(self):
        broker = wijisqs.SqsBroker(
            aws_region_name="eu-west-1",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="12331414",
        )
        self.assertTrue(isinstance(broker, wiji.broker.BaseBroker))

    def test_task_queuing(self):
        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            brokers = [
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=False,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=True,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=False,
                    batch_send=False,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=True,
                    batch_send=True,
                ),
            ]
            for broker in brokers:
                myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
                myAdderTask.synchronous_delay(4, 6, task_options=wiji.task.TaskOptions(eta=34.56))
                # TODO: asser things

    def test_task_dequeuing(self):
        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        kwargs = {"a": 78, "b": 101}
        proto = wiji.protocol.Protocol(
            version=1,
            task_id="task_id",
            eta="2019-03-12T14:21:27.751149+00:00",
            current_retries=0,
            max_retries=0,
            log_id="log_id",
            hook_metadata="hook_metadata",
            argsy=(),
            kwargsy=kwargs,
        )

        queue_tags = {
            "Name": "testSqs",
            "Owner": "testOwner",
            "Environment": "staging",
            "Classification": "internal-use-only",
            "Status": "deprecated",
        }
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs(proto=proto)

            brokers = [
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=False,
                    queue_tags=queue_tags,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=True,
                    queue_tags=queue_tags,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=False,
                    batch_send=False,
                    queue_tags=queue_tags,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=True,
                    batch_send=True,
                    queue_tags=queue_tags,
                ),
            ]

            for broker in brokers:
                # queue tasks
                myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
                myAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

                # consume tasks
                worker = wiji.Worker(the_task=myAdderTask, worker_id="TestWorkerID1")
                dequeued_item = self._run(worker.consume_tasks(TESTING=True))
                self.assertEqual(dequeued_item["version"], 1)
                self.assertEqual(dequeued_item["current_retries"], 0)
                self.assertEqual(dequeued_item["max_retries"], 0)
                self.assertEqual(dequeued_item["args"], [])
                self.assertEqual(dequeued_item["kwargs"], kwargs)

    def test_receive_no_message(self):
        mock_okay_resp = {
            "Messages": [
                {
                    "MessageId": "MOCK_MessageId-0",
                    "ReceiptHandle": "MOCK_ReceiptHandle-0",
                    "MD5OfBody": "MOCK_MD5OfBody-0",
                    "Body": "self.proto.json()",
                    "Attributes": {"MOCK_Attributes_KEY-0": "MOCK_Attributes_Value-0"},
                    "MD5OfMessageAttributes": "MOCK_MD5OfMessageAttributes-0",
                    "MessageAttributes": {
                        "task_id": {"StringValue": "MOCK_task_id-0", "DataType": "string"},
                        "task_eta": {"StringValue": "MOCK_task_eta-0", "DataType": "string"},
                        "task_hook_metadata": {
                            "StringValue": "MOCK_task_hook_metadata-0",
                            "DataType": "string",
                        },
                    },
                }
            ]
        }

        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            # okay response
            mock_boto_client.return_value = MockSqs(mock_msg_to_receive=mock_okay_resp)
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=False,
            )
            msg = broker._receive_message(queue_name="TestQueue")
            self.assertEqual(msg, mock_okay_resp["Messages"][0]["Body"])
            self.assertEqual(broker.recieveBuf.size(), 0)

        mock_empty_resp = {
            "ResponseMetadata": {
                "RequestId": "90deaad3-aae8-53d1-be1b-ffd944323f5a",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amzn-requestid": "90deaad3-aae8-53d1-be1b-ffd944323f5a",
                    "date": "Tue, 19 Mar 2019 13:20:46 GMT",
                    "content-type": "text/xml",
                    "content-length": "240",
                },
                "RetryAttempts": 0,
            }
        }
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            # empty response
            mock_boto_client.return_value = MockSqs(mock_msg_to_receive=mock_empty_resp)

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=True,
            )
            msg = broker._receive_message(queue_name="TestQueue")
            self.assertEqual(msg, None)

    def test_create_queue_called_once(self):
        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        class PrintTask(wiji.task.Task):
            async def run(self):
                print("PrintTask executed")

        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()
            # mock_create_queue.return_value = None

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=False,
            )

            # queue tasks
            myAdderTask = AdderTask(
                the_broker=broker, queue_name="AdderTask_test_create_queue_called_once"
            )
            myPrintTask = PrintTask(
                the_broker=broker, queue_name="PrintTask_test_create_queue_called_once"
            )
            myAdderTask.synchronous_delay(a=23, b=14)
            myPrintTask.synchronous_delay()

            # TODO: implement this
            print("# TODO: implement this")

    def test_queurl_available(self):
        class PrintTask(wiji.task.Task):
            async def run(self, **kwargs):
                print("PrintTask executed")

        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
            )
            # queue tasks
            myPrintTask = PrintTask(
                the_broker=broker, queue_name="PrintTask_test_create_queue_called_once"
            )
            myPrintTask.synchronous_delay()
            self.assertIsNotNone(broker.QueueUrl)
            self.assertIsInstance(broker.QueueUrl, str)

    def test_retries(self):
        res = wijisqs.SqsBroker._retry_after(-110)
        self.assertEqual(res, 30)

        res = wiji.Worker._retry_after(5)
        self.assertTrue(res > 60 * (2 ** 5))

        for i in [6, 7, 34]:
            res = wiji.Worker._retry_after(i)
            self.assertTrue(res > 16 * 60)

    def test_retries_called(self):
        mock_empty_resp = {
            "ResponseMetadata": {
                "RequestId": "90deaad3-aae8-53d1-be1b-ffd944323f5a",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amzn-requestid": "90deaad3-aae8-53d1-be1b-ffd944323f5a",
                    "date": "Tue, 19 Mar 2019 13:20:46 GMT",
                    "content-type": "text/xml",
                    "content-length": "240",
                },
                "RetryAttempts": 0,
            }
        }
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._retry_after"
        ) as mock_retry_after:
            # empty response
            mock_boto_client.return_value = MockSqs(mock_msg_to_receive=mock_empty_resp)
            mock_retry_after.return_value = 1

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=False,
            )
            msg = self._run(broker.dequeue(queue_name="TestQueue", TESTING=True))
            self.assertEqual(msg, '{"key": "mock_item"}')
            self.assertTrue(mock_retry_after.called)


class TestBatching(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_broker.TestBatching.test_something
    """

    def setUp(self):
        self.queue_name = "WijiSqsTestBatchingQueue"

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_no_batching(self):
        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=False,
            )

            myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
            myAdderTask.synchronous_delay(
                a=kwargsy["a"], b=kwargsy["b"], task_options=wiji.task.TaskOptions(eta=34.56)
            )

            self.assertTrue(mock_send_message.called)
            self.assertEqual(json.loads(mock_send_message.call_args[1]["item"])["kwargs"], kwargsy)
            self.assertFalse(mock_send_message_batch.called)
            self.assertEqual(broker.sendBuf.size(), 0)

    def test_yes_batching_one_message(self):
        """
        test that even when batching is ON, no message is sent to AWS.
        instead it goes to buffer
        """

        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
            )

            myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
            myAdderTask.synchronous_delay(
                a=kwargsy["a"], b=kwargsy["b"], task_options=wiji.task.TaskOptions(eta=34.56)
            )

            self.assertFalse(mock_send_message.called)
            self.assertFalse(mock_send_message_batch.called)
            self.assertEqual(broker.sendBuf.size(), 1)

    def test_yes_batching_13_message(self):
        """
        test that even when batching is ON, and we have more than 10messages; some are sent to AWS
        and the remainder are left
        """

        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
            )

            myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
            num_msgs = []
            for _ in range(1, 15):
                num_msgs.append(1)
                myAdderTask.synchronous_delay(a=kwargsy["a"], b=kwargsy["b"])
            self.assertEqual(len(num_msgs), 14)

            self.assertFalse(mock_send_message.called)
            self.assertTrue(mock_send_message_batch.called)
            self.assertEqual(len(mock_send_message_batch.call_args[1]["Entries"]), 10)
            self.assertEqual(
                json.loads(mock_send_message_batch.call_args[1]["Entries"][9]["MessageBody"])[
                    "kwargs"
                ],
                kwargsy,
            )
            # 4 messages are left
            self.assertEqual(broker.sendBuf.size(), 4)

    def test_yes_batching_one_message_long_duration(self):
        """
        test that when batching is ON, and there's only one message in buffer;
        if the duration is longer than `batching_duration`, then the one message is sent to AWS
        and the one incoming message saved to buffer
        """

        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()

            batching_duration = 0.11
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
            myAdderTask.synchronous_delay(a=kwargsy["a"], b=kwargsy["b"])

            time.sleep(batching_duration * 2)
            myAdderTask.synchronous_delay(a=kwargsy["a"], b=kwargsy["b"])

            self.assertFalse(mock_send_message.called)
            self.assertTrue(mock_send_message_batch.called)
            self.assertEqual(len(mock_send_message_batch.call_args[1]["Entries"]), 1)
            self.assertEqual(
                json.loads(mock_send_message_batch.call_args[1]["Entries"][0]["MessageBody"])[
                    "kwargs"
                ],
                kwargsy,
            )
            # no messages left
            self.assertEqual(broker.sendBuf.size(), 1)


class TestLongPoll(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_broker.TestLongPoll.test_something
    """

    def setUp(self):
        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        self.AdderTask = AdderTask
        self.queue_name = "WijiSqsTestQueue"

        self.kwargsy = {"a": 78, "b": 101}
        self.proto = wiji.protocol.Protocol(
            version=1,
            task_id="task_id",
            eta="2019-03-12T14:21:27.751149+00:00",
            current_retries=0,
            max_retries=0,
            log_id="log_id",
            hook_metadata="hook_metadata",
            argsy=(),
            kwargsy=self.kwargsy,
        )

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_no_poll(self):
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._receive_message_POLL"
        ) as mock_receive_message_POLL, mock.patch(
            "wijisqs.SqsBroker._receive_message_NO_poll"
        ) as mock_receive_message_NO_poll:
            mock_boto_client.return_value = MockSqs(proto=self.proto)
            mock_receive_message_NO_poll.return_value = self.proto.json()

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=False,
            )

            # queue tasks
            myAdderTask = self.AdderTask(the_broker=broker, queue_name=self.queue_name)
            myAdderTask.synchronous_delay(a=self.kwargsy["a"], b=self.kwargsy["b"])

            # consume tasks
            worker = wiji.Worker(the_task=myAdderTask, worker_id="TestWorkerID1")
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertEqual(dequeued_item["current_retries"], 0)
            self.assertEqual(dequeued_item["max_retries"], 0)
            self.assertEqual(dequeued_item["args"], [])
            self.assertEqual(dequeued_item["kwargs"], self.kwargsy)

            self.assertFalse(mock_receive_message_POLL.called)
            self.assertTrue(mock_receive_message_NO_poll.called)
            self.assertEqual(broker.recieveBuf.size(), 0)

    #######
    def test_yes_poll(self):
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs(proto=self.proto)
            # mock_receive_message_POLL.return_value = self.proto.json()

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=True,
            )

            # queue tasks
            myAdderTask = self.AdderTask(the_broker=broker, queue_name=self.queue_name)
            num_msgs = []
            for _ in range(1, 15):
                num_msgs.append(1)
                myAdderTask.synchronous_delay(a=self.kwargsy["a"], b=self.kwargsy["b"])
            self.assertEqual(len(num_msgs), 14)

            # consume tasks
            worker = wiji.Worker(the_task=myAdderTask, worker_id="TestWorkerID1")
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertEqual(dequeued_item["current_retries"], 0)
            self.assertEqual(dequeued_item["max_retries"], 0)
            self.assertEqual(dequeued_item["args"], [])
            self.assertEqual(dequeued_item["kwargs"], self.kwargsy)
            # SQS only returns `broker.MaxNumberOfMessages` number of messages upto a maximum of 10
            # and then one message gets consumed by `wiji`. So number left in buffer is broker.MaxNumberOfMessages-1
            self.assertEqual(broker.recieveBuf.size(), (broker.MaxNumberOfMessages - 1))
