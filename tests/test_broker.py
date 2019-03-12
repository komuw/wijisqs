import sys
import asyncio
import logging
from unittest import TestCase, mock

import wiji
import wijisqs
from botocore.stub import Stubber

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)


def AsyncMock(*args, **kwargs):
    """
    see: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


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

    @staticmethod
    def _stub_sqs_task_queuing(broker):
        stubber = Stubber(broker.client)
        stubber.deactivate()

        mock_create_queue_resp = {"QueueUrl": "mock_url_value"}
        stubber.add_response("create_queue", mock_create_queue_resp)

        mock_tag_queue_resp = {
            "ResponseMetadata": {"RequestId": "abc123", "HTTPStatusCode": 200, "HostId": "abc123"}
        }
        stubber.add_response("tag_queue", mock_tag_queue_resp)

        mock_send_message_resp = {
            "MD5OfMessageBody": "string",
            "MD5OfMessageAttributes": "string",
            "MessageId": "string",
            "SequenceNumber": "string",
        }
        stubber.add_response("send_message", mock_send_message_resp)
        return stubber

    @staticmethod
    def _stub_sqs_dequeing(broker, proto):
        stubber = Stubber(broker.client)
        stubber.deactivate()

        mock_create_queue_resp = {"QueueUrl": "mock_url_value"}
        stubber.add_response("create_queue", mock_create_queue_resp)

        mock_tag_queue_resp = {
            "ResponseMetadata": {"RequestId": "abc123", "HTTPStatusCode": 200, "HostId": "abc123"}
        }
        stubber.add_response("tag_queue", mock_tag_queue_resp)

        mock_send_message_resp = {
            "MD5OfMessageBody": "string",
            "MD5OfMessageAttributes": "string",
            "MessageId": "string",
            "SequenceNumber": "string",
        }
        stubber.add_response("send_message", mock_send_message_resp)

        mock_create_queue_resp = {"QueueUrl": "mock_url_value"}
        stubber.add_response("create_queue", mock_create_queue_resp)

        mock_receive_message_resp = {
            "Messages": [
                {
                    "MessageId": "string",
                    "ReceiptHandle": "string",
                    "MD5OfBody": "string",
                    "Body": proto.json(),
                    "Attributes": {"string": "string"},
                    "MD5OfMessageAttributes": "string",
                    "MessageAttributes": {
                        "task_id": {"StringValue": "task_id", "DataType": "string"},
                        "task_eta": {"StringValue": "task_eta", "DataType": "string"},
                        "task_hook_metadata": {
                            "StringValue": "task_hook_metadata",
                            "DataType": "string",
                        },
                    },
                }
            ]
        }
        stubber.add_response("receive_message", mock_receive_message_resp)
        return stubber

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
        broker = wijisqs.SqsBroker(
            aws_region_name="eu-west-1",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="aws_secret_access_key",
            loglevel="DEBUG",
        )
        stubber = self._stub_sqs_task_queuing(broker=broker)
        with stubber:

            class AdderTask(wiji.task.Task):
                async def run(self, a, b):
                    res = a + b
                    return res

            myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
            myAdderTask.synchronous_delay(4, 6, task_options=wiji.task.TaskOptions(eta=34.56))
            stubber.assert_no_pending_responses()

    def test_task_dequeuing(self):
        broker = wijisqs.SqsBroker(
            aws_region_name="eu-west-1",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="aws_secret_access_key",
            loglevel="DEBUG",
            queue_tags={
                "Name": "testSqs",
                "Owner": "testOwner",
                "Environment": "staging",
                "Classification": "internal-use-only",
                "Status": "deprecated",
            },
        )
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
        stubber = self._stub_sqs_dequeing(broker=broker, proto=proto)
        with stubber:

            class AdderTask(wiji.task.Task):
                async def run(self, a, b):
                    res = a + b
                    return res

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

            stubber.assert_no_pending_responses()
