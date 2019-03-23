## `wijisqs` changelog:
most recent version is listed first.

## **version:** v0.2.2
- bugfix; broker drain duration ought to be a float.

## **version:** v0.2.1
implemented clean shutdown: https://github.com/komuw/wijisqs/pull/32

## **version:** v0.2.0
- shard/separate all state per queue: https://github.com/komuw/wijisqs/pull/30
- use one `botocore` client per thread: https://github.com/komuw/wijisqs/pull/31

## **version:** v0.1.8
- one `wijisqs.SqsBroker` instance should be able to serve multiple queues: https://github.com/komuw/wijisqs/pull/26

## **version:** v0.1.6
- bugfix, make sure QueueUrl is always available: https://github.com/komuw/wijisqs/pull/23
- for empty queues, sleep exponetially upto maximum : https://github.com/komuw/wijisqs/pull/24

## **version:** v0.1.5
- bugfix; fix keyError on empty queues : https://github.com/komuw/wijisqs/pull/18
- for the same instance of a `wijisqs.SqsBroker`; create queue and tag queue should only run once : https://github.com/komuw/wijisqs/pull/19

## **version:** v0.1.4
- use the current eventloop if available : https://github.com/komuw/wijisqs/pull/15

## **version:** v0.1.2
- default `batch_send` to False: https://github.com/komuw/wijisqs/pull/12

## **version:** v0.1.1
- make task buffers be thread safe: https://github.com/komuw/wijisqs/pull/11     
