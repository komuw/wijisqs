import sys
import time
import asyncio
import logging
import datetime
from unittest import TestCase, mock

import wiji
import wijisqs


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

    def test_bad_instantiation(self):
        def create_broker():
            broker = wijisqs.SqsBroker(
                region_name="region_name",
                aws_access_key_id="aws_access_key_id",
                aws_secret_access_key=12331414,
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
            region_name="eu-west-1",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="12331414",
        )
        self.assertTrue(isinstance(broker, wiji.broker.BaseBroker))

    def test_task_queuing(self):
        broker = wijisqs.SqsBroker(
            region_name="eu-west-1",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="aws_secret_access_key",
            loglevel="DEBUG",
        )

        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
        myAdderTask.synchronous_delay(4, 6, task_options=wiji.task.TaskOptions(eta=34.56))

    def test_task_dequeuing(self):
        broker = wijisqs.SqsBroker(
            region_name="eu-west-1",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="aws_secret_access_key",
            loglevel="DEBUG",
        )

        class AdderTask(wiji.task.Task):
            async def run(self, a, b):
                res = a + b
                return res

        myAdderTask = AdderTask(the_broker=broker, queue_name=self.queue_name)
        worker = wiji.Worker(the_task=myAdderTask, worker_id="TestWorkerID1")

        # queue tasks
        kwargs = {"a": 78, "b": 101}
        myAdderTask.synchronous_delay(a=kwargs["a"], b=kwargs["b"])

        # consume tasks
        dequeued_item = self._run(worker.consume_tasks(TESTING=True))
        self.assertEqual(dequeued_item["version"], 1)
        self.assertEqual(dequeued_item["current_retries"], 0)
        self.assertEqual(dequeued_item["max_retries"], 0)
        self.assertEqual(dequeued_item["args"], [])
        self.assertEqual(dequeued_item["kwargs"], kwargs)
