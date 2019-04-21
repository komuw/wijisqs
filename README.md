## wijisqs             


[![Codacy Badge](https://api.codacy.com/project/badge/Grade/bc260950808a481db69c310fc0f05eb2)](https://www.codacy.com/app/komuw/wijisqs)
[![CircleCI](https://circleci.com/gh/komuw/wijisqs.svg?style=svg)](https://circleci.com/gh/komuw/wijisqs)
[![codecov](https://codecov.io/gh/komuw/wijisqs/branch/master/graph/badge.svg)](https://codecov.io/gh/komuw/wijisqs)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/komuw/wijisqs)


[wiji](https://github.com/komuw/wiji) is an asyncio distributed task processor/queue.       


`wijisqs` on the other hand, is an AWS SQS broker for [wiji](https://github.com/komuw/wiji)
  

**Contents:**          
[Installation](#installation)         
[Usage](#usage)                  
  + [As a library](#1-as-a-library)            
  + [As cli app](#2-as-a-cli-app)            

## Installation

```shell
pip install wijisqs
```           


## Usage

#### 1. As a library
```python
import os
import wiji
import wijisqs
import asyncio

broker = wijisqs.SqsBroker(
                aws_region_name="aws_region_name",
                aws_access_key_id=os.environ.get("aws_access_key_id"),
                aws_secret_access_key=os.environ.get("aws_secret_access_key"),
            )

class AdderTask(wiji.task.Task):
    the_broker = broker
    queue_name = "AdderTaskQueue1"

    async def run(self, a, b):
        result = a + b
        print("\nresult: {0}\n".format(result))
        return result

# queue some tasks
myAdderTask = AdderTask()
myAdderTask.synchronous_delay(a=4, b=37)
myAdderTask.synchronous_delay(a=67, b=847)

# run the workers
worker = wiji.Worker(the_task=myAdderTask)
asyncio.run(worker.consume_tasks())
```


For extended documentation, see the [wiji documentation](https://github.com/komuw/wiji) 