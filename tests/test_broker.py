import time
import json
import asyncio
import threading
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
            self.proto = self.mock_proto()

        self.mock_msg_to_receive = mock_msg_to_receive

    @staticmethod
    def mock_proto():
        task_options = wiji.task.TaskOptions(eta=0.00, max_retries=0, hook_metadata="hook_metadata")
        task_options.task_id = "Mock_taskID"
        return wiji.protocol.Protocol(version=1, task_options=task_options)

    def create_queue(self, *args, **kwargs):
        queue_name = kwargs["QueueName"]
        return {"QueueUrl": "http://mock_{queue_name}_SQS_QueueUrl".format(queue_name=queue_name)}

    def tag_queue(self, *args, **kwargs):
        return {}

    def get_queue_url(self, *args, **kwargs):
        queue_name = kwargs["QueueName"]
        return {"QueueUrl": "http://mock_{queue_name}_SQS_QueueUrl".format(queue_name=queue_name)}

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
        pass

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
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker.enqueue", new=AsyncMock()
        ) as mock_enqueue:
            mock_boto_client.return_value = MockSqs()
            mock_enqueue.mock.return_value = None

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

                class AdderTask(wiji.task.Task):
                    the_broker = broker
                    queue_name = "AdderTaskQueue-test_task_queuing"

                    async def run(self, a, b):
                        res = a + b
                        return res

                myAdderTask = AdderTask()
                myAdderTask.synchronous_delay(4, 6, task_options=wiji.task.TaskOptions(eta=34.56))
                self.assertTrue(mock_enqueue.mock.called)
                self.assertEqual(mock_enqueue.mock.call_args[1]["queue_name"], AdderTask.queue_name)

    def test_task_dequeuing(self):
        def mock_okay_resp(task_id, Body):
            return {
                "Messages": [
                    {
                        "MessageId": "MOCK_MessageId-0",
                        "ReceiptHandle": "MOCK_ReceiptHandle-0",
                        "MD5OfBody": "MOCK_MD5OfBody-0",
                        "Body": Body,
                        "Attributes": {"MOCK_Attributes_KEY-0": "MOCK_Attributes_Value-0"},
                        "MD5OfMessageAttributes": "MOCK_MD5OfMessageAttributes-0",
                        "MessageAttributes": {
                            "task_id": {"StringValue": task_id, "DataType": "string"},
                            "task_eta": {"StringValue": "MOCK_task_eta-0", "DataType": "string"},
                            "task_hook_metadata": {
                                "StringValue": "MOCK_task_hook_metadata-0",
                                "DataType": "string",
                            },
                        },
                    }
                ]
            }

        kwargs = {"a": 78, "b": 101}
        body = {
            "version": 1,
            "task_options": {
                "eta": "2019-03-29T13:39:49.322722+00:00",
                "task_id": "mock_task_id",
                "current_retries": 0,
                "max_retries": 0,
                "hook_metadata": "hook_metadata",
                "args": [],
                "kwargs": kwargs,
            },
        }

        queue_tags = {
            "Name": "testSqs",
            "Owner": "testOwner",
            "Environment": "staging",
            "Classification": "internal-use-only",
            "Status": "deprecated",
        }
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs(
                mock_msg_to_receive=mock_okay_resp(
                    task_id=body["task_options"]["task_id"], Body=json.dumps(body)
                )
            )

            brokers = [
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=False,
                    queue_tags=queue_tags,
                    batching_duration=0.0001,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=True,
                    queue_tags=queue_tags,
                    batching_duration=0.0001,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=False,
                    batch_send=False,
                    queue_tags=queue_tags,
                    batching_duration=0.0001,
                ),
                wijisqs.SqsBroker(
                    aws_region_name="eu-west-1",
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    loglevel="DEBUG",
                    long_poll=True,
                    batch_send=True,
                    queue_tags=queue_tags,
                    batching_duration=0.0001,
                ),
            ]

            for broker in brokers:

                class AdderTask(wiji.task.Task):
                    the_broker = broker
                    queue_name = "AdderTaskQueue-test_task_dequeuing"

                    async def run(self, a, b):
                        res = a + b
                        return res

                # queue tasks
                myAdderTask = AdderTask()
                myAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

                # consume tasks
                worker = wiji.Worker(the_task=myAdderTask, worker_id="TestWorkerID1")
                dequeued_item = self._run(worker.consume_tasks(TESTING=True))
                self.assertEqual(dequeued_item["version"], 1)
                self.assertEqual(dequeued_item["task_options"]["current_retries"], 0)
                self.assertEqual(dequeued_item["task_options"]["max_retries"], 0)
                self.assertEqual(dequeued_item["task_options"]["args"], [])
                self.assertEqual(dequeued_item["task_options"]["kwargs"], kwargs)
                time.sleep(broker.batching_duration * 2)

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

        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            # okay response
            mock_boto_client.return_value = MockSqs(mock_msg_to_receive=mock_okay_resp)
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=False,
            )
            broker._get_queue_url(queue_name="TestQueue")
            msg = broker._receive_message(queue_name="TestQueue")
            self.assertEqual(msg, mock_okay_resp["Messages"][0]["Body"])
            self.assertEqual(broker._get_per_queue_recieveBuf("TestQueue").size(), 0)

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
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            # empty response
            mock_boto_client.return_value = MockSqs(mock_msg_to_receive=mock_empty_resp)

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=True,
            )
            broker._get_queue_url(queue_name="TestQueue")
            msg = broker._receive_message(queue_name="TestQueue")
            self.assertEqual(msg, None)

    def test_create_queue_called_once(self):
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=False,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTaskQueue-test_create_queue_called_once"

                async def run(self, a, b):
                    res = a + b
                    return res

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask"

                async def run(self):
                    print("PrintTask executed")

            # queue tasks
            AdderTask().synchronous_delay(a=23, b=14)
            PrintTask().synchronous_delay()

            # TODO: implement this
            print("# TODO: implement this")

    def test_queurl_available(self):
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            # queue tasks
            PrintTask().synchronous_delay()
            self.assertIsNotNone(broker._get_per_queue_url(queue_name=PrintTask.queue_name))
            self.assertIsInstance(broker._get_per_queue_url(queue_name=PrintTask.queue_name), str)

    def test_retries(self):
        broker = wijisqs.SqsBroker(
            aws_region_name="eu-west-1",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="aws_secret_access_key",
        )

        res = broker._retry_after(-110)
        self.assertEqual(res, broker.VisibilityTimeout / 5)

        res = wiji.Worker._retry_after(5)
        self.assertTrue(res > 60 * (2 ** 5))

        for i in [6, 7, 34]:
            res = broker._retry_after(i)
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
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._retry_after"
        ) as mock_retry_after:
            # empty response
            mock_boto_client.return_value = MockSqs(mock_msg_to_receive=mock_empty_resp)
            mock_retry_after.return_value = 1

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
            ]
            for broker in brokers:
                broker._get_queue_url(queue_name="TestQueue")
                msg = self._run(broker.dequeue(queue_name="TestQueue", TESTING=True))
                self.assertEqual(msg, '{"key": "mock_item"}')
                self.assertTrue(mock_retry_after.called)

    def test_multiple_queues_one_broker(self):
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask"

                async def run(self, a, b):
                    res = a + b
                    return res

            # queue tasks
            print_queue_name = PrintTask.queue_name
            myPrintTask = PrintTask()
            myPrintTask.synchronous_delay()
            myPrintTask.synchronous_delay()
            myPrintTask.synchronous_delay()
            adder_queue_name = AdderTask.queue_name
            myAdderTask = AdderTask()
            myAdderTask.synchronous_delay(a=12, b=55)
            myAdderTask.synchronous_delay(a=2, b=55)
            myAdderTask.synchronous_delay(a=133, b=545)
            myAdderTask.synchronous_delay(a=0, b=5005)

            self.assertEqual(len(broker._PER_QUEUE_STATE), 2)
            self.assertIsInstance(broker._get_per_queue_url(print_queue_name), str)
            self.assertIsInstance(broker._get_per_queue_url(adder_queue_name), str)
            self.assertIn(print_queue_name, broker._get_per_queue_url(print_queue_name))
            self.assertIn(adder_queue_name, broker._get_per_queue_url(adder_queue_name))

    def test_task_deletion(self):
        def mock_okay_resp(task_id, Body):
            return {
                "Messages": [
                    {
                        "MessageId": "MOCK_MessageId-0",
                        "ReceiptHandle": "MOCK_ReceiptHandle-0",
                        "MD5OfBody": "MOCK_MD5OfBody-0",
                        "Body": Body,
                        "Attributes": {"MOCK_Attributes_KEY-0": "MOCK_Attributes_Value-0"},
                        "MD5OfMessageAttributes": "MOCK_MD5OfMessageAttributes-0",
                        "MessageAttributes": {
                            "task_id": {"StringValue": task_id, "DataType": "string"},
                            "task_eta": {"StringValue": "MOCK_task_eta-0", "DataType": "string"},
                            "task_hook_metadata": {
                                "StringValue": "MOCK_task_hook_metadata-0",
                                "DataType": "string",
                            },
                        },
                    }
                ]
            }

        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            # queue tasks
            myPrintTask = PrintTask()
            myPrintTask.synchronous_delay()
            body = {
                "version": 1,
                "task_options": {
                    "eta": "2019-03-29T13:39:49.322722+00:00",
                    "task_id": myPrintTask._get_task_options().task_id,
                    "current_retries": 0,
                    "max_retries": 0,
                    "hook_metadata": "hook_metadata",
                    "args": [],
                    "kwargs": {},
                },
            }
            mock_boto_client.return_value = MockSqs(
                mock_msg_to_receive=mock_okay_resp(
                    task_id=body["task_options"]["task_id"], Body=json.dumps(body)
                )
            )
            # consume tasks
            worker = wiji.Worker(the_task=myPrintTask, worker_id="TestWorkerID1")
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)

            # item has been deleted
            # `Worker.worker.consume_tasks` calls `broker.done` at the end so broker should have deleted the message
            self.assertEqual(len(broker._get_per_queue_task_receipt(PrintTask.queue_name)), 0)

    def test_threading(self):
        # TODO: fix this test. it is racy

        # for this test we SHOULD NOT mock _get_per_thread_client
        with mock.patch("wijisqs.SqsBroker._sqs_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            self._run(broker.check(queue_name=PrintTask.queue_name))
            max_num_threads = 5
            thread_name = "test_threading-thread-prefix"

            def worker_thread(num):
                current_thread_identity = threading.get_ident()
                broker._tag_queue(queue_name=PrintTask.queue_name)
                self.assertIsNotNone(broker._PER_THREAD_STATE.get(current_thread_identity))
                thread_names = []
                for t in threading.enumerate():
                    thread_names.append(t.getName())
                self.assertTrue(thread_name in thread_names)

                if num == (max_num_threads - 1):
                    self.assertTrue(len(broker._PER_THREAD_STATE) >= 1)
                    self.assertTrue(threading.active_count() > 1)
                return

            for i in range(max_num_threads):
                t = threading.Thread(target=worker_thread, name=thread_name, kwargs={"num": i})
                t.start()


class TestBatching(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_broker.TestBatching.test_something

    For most of the tests in this class, we need to;
      `time.sleep()` for a duration longer than `timer.WijiSqsTimer` will be around.
    This will ensure that;
      (i) `timer.WijiSqsTimer` has had time to run and thus drain the `sendBuf`
      (ii) the `mock.patch("wijisqs.SqsBroker._get_per_thread_client")` is still arrive and thus `WijiSqsTimer` doesn't make actual AWS calls
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_no_batching(self):
        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()
            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=False,
                batching_duration=batching_duration,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask-test_no_batching"

                async def run(self, a, b):
                    res = a + b
                    return res

            AdderTask().synchronous_delay(
                a=kwargsy["a"], b=kwargsy["b"], task_options=wiji.task.TaskOptions(eta=34.56)
            )

            self.assertTrue(mock_send_message.called)
            self.assertEqual(
                json.loads(mock_send_message.call_args[1]["item"])["task_options"]["kwargs"],
                kwargsy,
            )
            self.assertFalse(mock_send_message_batch.called)
            self.assertEqual(broker._get_per_queue_sendBuf(AdderTask.queue_name).size(), 0)

    def test_yes_batching_one_message(self):
        """
        test that even when batching is ON, no message is sent to AWS.
        instead it goes to buffer
        """
        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()
            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask-test_yes_batching_one_message"

                async def run(self, a, b):
                    res = a + b
                    return res

            AdderTask().synchronous_delay(
                a=kwargsy["a"], b=kwargsy["b"], task_options=wiji.task.TaskOptions(eta=34.56)
            )

            self.assertFalse(mock_send_message.called)
            self.assertFalse(mock_send_message_batch.called)
            self.assertEqual(broker._get_per_queue_sendBuf(AdderTask.queue_name).size(), 1)

    def test_yes_batching_13_message(self):
        """
        test that even when batching is ON, and we have more than 10messages; some are sent to AWS
        while others are left in the buffer.
        """
        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()
            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask-test_yes_batching_13_message"

                async def run(self, a, b):
                    res = a + b
                    return res

            num_msgs = []
            for _ in range(1, 15):
                num_msgs.append(1)
                AdderTask().synchronous_delay(a=kwargsy["a"], b=kwargsy["b"])
            self.assertEqual(len(num_msgs), 14)

            self.assertFalse(mock_send_message.called)
            self.assertTrue(mock_send_message_batch.called)
            self.assertEqual(
                json.loads(mock_send_message_batch.call_args[1]["Entries"][0]["MessageBody"])[
                    "task_options"
                ]["kwargs"],
                kwargsy,
            )
            # 1 message is left
            self.assertEqual(broker._get_per_queue_sendBuf(AdderTask.queue_name).size(), 1)

    def test_yes_batching_13_message_await_duration(self):
        """
        test that even when batching is ON, and we have more than 10messages; some are sent to AWS
        via normal means while others are sent by the background thread.
        Thus no msg(after a certain period of time) is left in the buffers.
        """
        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()
            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask-test_yes_batching_13_message_await_duration"

                async def run(self, a, b):
                    res = a + b
                    return res

            num_msgs = []
            for _ in range(1, 15):
                num_msgs.append(1)
                AdderTask().synchronous_delay(a=kwargsy["a"], b=kwargsy["b"])
            self.assertEqual(len(num_msgs), 14)

            self.assertFalse(mock_send_message.called)
            self.assertTrue(mock_send_message_batch.called)
            self.assertEqual(
                json.loads(mock_send_message_batch.call_args[1]["Entries"][0]["MessageBody"])[
                    "task_options"
                ]["kwargs"],
                kwargsy,
            )

            time.sleep(batching_duration * 2)
            # No message is left
            self.assertEqual(broker._get_per_queue_sendBuf(AdderTask.queue_name).size(), 0)

    def test_yes_batching_one_message_long_duration(self):
        """
        test that when batching is ON, and there's only one message in buffer;
        if the duration is longer than `batching_duration`, then the one message is sent to AWS
        and the one incoming message saved to buffer
        """
        kwargsy = {"a": 4, "b": 6}
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
            "wijisqs.SqsBroker._send_message"
        ) as mock_send_message, mock.patch(
            "wijisqs.SqsBroker._send_message_batch"
        ) as mock_send_message_batch:
            mock_boto_client.return_value = MockSqs()

            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask-test_yes_batching_one_message_long_duration"

                async def run(self, a, b):
                    res = a + b
                    return res

            myAdderTask = AdderTask()
            # this one message will go into buffer
            myAdderTask.synchronous_delay(a=kwargsy["a"], b=kwargsy["b"])

            time.sleep(batching_duration * 2)
            myAdderTask.synchronous_delay(a=kwargsy["a"], b=kwargsy["b"])

            self.assertFalse(mock_send_message.called)
            self.assertTrue(mock_send_message_batch.called)
            self.assertEqual(len(mock_send_message_batch.call_args[1]["Entries"]), 1)
            self.assertEqual(
                json.loads(mock_send_message_batch.call_args[1]["Entries"][0]["MessageBody"])[
                    "task_options"
                ]["kwargs"],
                kwargsy,
            )
            # 1 messages left
            self.assertEqual(broker._get_per_queue_sendBuf(AdderTask.queue_name).size(), 1)

            # sleep so that `WijiSqsTimer` can run and also
            # so that `_get_per_thread_client` patch is still in scope
            time.sleep(batching_duration * 2)
            self.assertEqual(
                broker._get_per_queue_sendBuf(AdderTask.queue_name).size(), 0
            )  # remaining msg was sent to AWS by `WijiSqsTimer`

    def test_batch_send_less_messages(self):
        """
        If we have batch_send=True:
          and then we queue only 5 messages(ie less than 10), and we do not queue any other messages,
          those 5 SHOULD still be sent to SQS after expiry of `batching_duration`.
         This test guards against regressing to a bug that existed in wijisqs: https://github.com/komuw/wijisqs/issues/36
        """
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask-test_batch_send_less_messages"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            # queue any messages less than 10
            for i in range(0, 5):
                PrintTask().synchronous_delay()

            # sleep so that batching_duration expires
            time.sleep(batching_duration * 5)

            # all messages should have been sent to SQS,
            # even if we haven't queued a new one.
            self.assertEqual(broker._get_per_queue_sendBuf(PrintTask.queue_name).size(), 0)

    def test_batch_send_less_messages_duration_not_expired(self):
        """
        If we have batch_send=True:
          and then we queue only 5 messages(ie less than 10), and we do not queue any other messages,
          those 5 SHOULD still be sent to SQS after expiry of `batching_duration`.
          This test guards against regressing to a bug that existed in wijisqs: https://github.com/komuw/wijisqs/issues/36
        """
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask-test_batch_send_less_messages_duration_not_expired"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            # queue any messages less than 10
            for i in range(0, 5):
                PrintTask().synchronous_delay()

            # if `broker.batching_duration` has not expired,
            # `broker._drain_sendBuf` does not run.
            self.assertEqual(broker._get_per_queue_sendBuf(PrintTask.queue_name).size(), 1)

    def test_batch_send_one_message(self):
        """
         This test guards against regressing to a bug that existed in wijisqs: https://github.com/komuw/wijisqs/issues/36
        """
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask-test_batch_send_one_message"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            # queue just one message
            PrintTask().synchronous_delay()
            time.sleep(batching_duration * 3)
            self.assertEqual(broker._get_per_queue_sendBuf(PrintTask.queue_name).size(), 0)

    def test_batch_send_one_message_plus_one(self):
        """
         This test guards against regressing to a bug that existed in wijisqs: https://github.com/komuw/wijisqs/issues/36
        """
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask-test_batch_send_one_message_plus_one"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            # queue just one message
            PrintTask().synchronous_delay()
            time.sleep(batching_duration * 2)
            self.assertEqual(broker._get_per_queue_sendBuf(PrintTask.queue_name).size(), 0)

            PrintTask().synchronous_delay()
            time.sleep(batching_duration * 2)
            self.assertEqual(broker._get_per_queue_sendBuf(PrintTask.queue_name).size(), 0)

    def test_batch_send_one_message_plus_one_multiple_queues(self):
        """
         This test guards against regressing to a bug that existed in wijisqs: https://github.com/komuw/wijisqs/issues/36
        """
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()

            batching_duration = 0.001
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                batch_send=True,
                batching_duration=batching_duration,
            )

            class PrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "PrintTask-test_batch_send_one_message_plus_one_multiple_queues"

                async def run(self, **kwargs):
                    print("PrintTask executed")

            class SecondPrintTask(wiji.task.Task):
                the_broker = broker
                queue_name = "SecondPrintTask-test_batch_send_one_message_plus_one_multiple_queues"

                async def run(self, **kwargs):
                    print("SecondPrintTask executed")

            # queue just one message of `PrintTask`
            PrintTask().synchronous_delay()
            time.sleep(batching_duration * 9)
            self.assertEqual(broker._get_per_queue_sendBuf(PrintTask.queue_name).size(), 0)

            # queue another of a different task, `SecondPrintTask`
            SecondPrintTask().synchronous_delay()
            time.sleep(batching_duration * 9)
            self.assertEqual(broker._get_per_queue_sendBuf(SecondPrintTask.queue_name).size(), 0)


class TestLongPoll(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_broker.TestLongPoll.test_something
    """

    def setUp(self):
        self.kwargsy = {"a": 78, "b": 101}

        task_options = wiji.task.TaskOptions(eta=0.00, max_retries=0, hook_metadata="hook_metadata")
        task_options.task_id = "Mock_taskID"
        task_options.args = ()
        task_options.kwargs = self.kwargsy
        self.proto = wiji.protocol.Protocol(version=1, task_options=task_options)

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_no_poll(self):
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client, mock.patch(
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

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask"

                async def run(self, a, b):
                    res = a + b
                    return res

            # queue tasks
            myAdderTask = AdderTask()
            myAdderTask.synchronous_delay(a=self.kwargsy["a"], b=self.kwargsy["b"])

            # consume tasks
            worker = wiji.Worker(the_task=myAdderTask, worker_id="TestWorkerID1")
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertEqual(dequeued_item["task_options"]["current_retries"], 0)
            self.assertEqual(dequeued_item["task_options"]["max_retries"], 0)
            self.assertEqual(dequeued_item["task_options"]["args"], [])
            self.assertEqual(dequeued_item["task_options"]["kwargs"], self.kwargsy)

            self.assertFalse(mock_receive_message_POLL.called)
            self.assertTrue(mock_receive_message_NO_poll.called)
            self.assertEqual(broker._get_per_queue_recieveBuf(AdderTask.queue_name).size(), 0)

    def test_yes_poll(self):
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs(proto=self.proto)
            # mock_receive_message_POLL.return_value = self.proto.json()

            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=True,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask"

                async def run(self, a, b):
                    res = a + b
                    return res

            # queue tasks
            myAdderTask = AdderTask()
            num_msgs = []
            for _ in range(1, 15):
                num_msgs.append(1)
                myAdderTask.synchronous_delay(a=self.kwargsy["a"], b=self.kwargsy["b"])
            self.assertEqual(len(num_msgs), 14)

            # consume tasks
            worker = wiji.Worker(the_task=myAdderTask, worker_id="TestWorkerID1")
            dequeued_item = self._run(worker.consume_tasks(TESTING=True))
            self.assertEqual(dequeued_item["version"], 1)
            self.assertEqual(dequeued_item["task_options"]["current_retries"], 0)
            self.assertEqual(dequeued_item["task_options"]["max_retries"], 0)
            self.assertEqual(dequeued_item["task_options"]["args"], [])
            self.assertEqual(dequeued_item["task_options"]["kwargs"], self.kwargsy)
            # SQS only returns `broker.MaxNumberOfMessages` number of messages upto a maximum of 10
            # and then one message gets consumed by `wiji`. So number left in buffer is broker.MaxNumberOfMessages-1
            self.assertEqual(
                broker._get_per_queue_recieveBuf(AdderTask.queue_name).size(),
                (broker.MaxNumberOfMessages - 1),
            )


class TestShutdown(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_broker.TestShutdown.test_something
    """

    def setUp(self):
        self.queue_name = "WijiSqsTestShutdownQueue"

    def tearDown(self):
        pass

    @staticmethod
    def _run(coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_shutdown(self):
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=True,
                batch_send=True,
            )

            class AdderTask(wiji.task.Task):
                the_broker = broker
                queue_name = "AdderTask"

                async def run(self, a, b):
                    res = a + b
                    return res

            self._run(broker.check(queue_name=self.queue_name))

            num_msgs_to_queue_in_buffer = 63
            sendBuf = broker._get_per_queue_sendBuf(queue_name=self.queue_name)
            for i in range(0, num_msgs_to_queue_in_buffer):
                # queue more than the usual 10 messages that a buffer usually has
                buffer_entry = {
                    "task_id": "myAdderTask.task_options.task_id",
                    "msg_body": "msg_body-{0}".format(i),
                    "delay": 1.0,
                    "eta": "myAdderTask.task_options.eta",
                    "task_hook_metadata": "hook_metadata",
                }
                sendBuf.put(new_item=buffer_entry)

            self.assertEqual(sendBuf.size(), num_msgs_to_queue_in_buffer)
            self._run(broker.shutdown(queue_name=self.queue_name, duration=1))
            self.assertEqual(sendBuf.size(), 0)

    def test_shutdown_multiple_queues(self):
        with mock.patch("wijisqs.SqsBroker._get_per_thread_client") as mock_boto_client:
            mock_boto_client.return_value = MockSqs()
            broker = wijisqs.SqsBroker(
                aws_region_name="eu-west-1",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key="aws_secret_access_key",
                loglevel="DEBUG",
                long_poll=True,
                batch_send=True,
            )
            self._run(broker.check(queue_name="AdderTaskQueue"))
            self._run(broker.check(queue_name="PrintTaskQueue"))

            num_msgs_to_queue_in_buffer = 63

            for q in ["AdderTaskQueue", "PrintTaskQueue"]:
                sendBuf = broker._get_per_queue_sendBuf(queue_name=q)
                for i in range(0, num_msgs_to_queue_in_buffer):
                    # queue more than the usual 10 messages that a buffer usually has
                    buffer_entry = {
                        "task_id": "task_id",
                        "msg_body": "msg_body-{0}".format(i),
                        "delay": "delay",
                        "eta": "eta",
                        "task_hook_metadata": "hook_metadata",
                    }
                    sendBuf.put(new_item=buffer_entry)

            self.assertEqual(
                broker._get_per_queue_sendBuf(queue_name="AdderTaskQueue").size(),
                num_msgs_to_queue_in_buffer,
            )
            self.assertEqual(
                broker._get_per_queue_sendBuf(queue_name="PrintTaskQueue").size(),
                num_msgs_to_queue_in_buffer,
            )
            self._run(broker.shutdown(queue_name="AdderTaskQueue", duration=1))
            self._run(broker.shutdown(queue_name="PrintTaskQueue", duration=1))
            self.assertEqual(broker._get_per_queue_sendBuf(queue_name="AdderTaskQueue").size(), 0)
            self.assertEqual(broker._get_per_queue_sendBuf(queue_name="PrintTaskQueue").size(), 0)
