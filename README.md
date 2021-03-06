Introduction
============

[![Build Status](https://secure.travis-ci.org/noodlefrenzy/node-sbus-qpid.png?branch=master)](https://travis-ci.org/noodlefrenzy/node-sbus-qpid)
[![Dependency Status](https://david-dm.org/noodlefrenzy/node-sbus-qpid.png)](https://david-dm.org/noodlefrenzy/node-sbus-qpid)

`node-sbus-amqp10` is a simple adapter you can pass to `node-sbus` to have it use the `node-qpid`
 module for all AMQP calls.  Since `node-qpid`, unlike `node-amqp-1-0`, has native code dependencies
 on Apache's Qpid Proton, it can only run on Linux/*nix systems (so far), but is likely to be faster than
 the "pure node" version (not yet verified).

 Details
 =======

 `node-sbus` relies on five simple methods to provide AMQP support - two for service bus, two for event hub, one for teardown:

* `send(uri, payload, cb)`
  * The URI should be the full AMQPS address you want to deliver to with included SAS name and key,
    e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/myqueue.
  * The payload is a JSON payload (which might get `JSON.stringify`'d), or a string.
  * The callback takes an error, and is called when the message is sent.
* `receive(uri, cb)`
  * The URI should be the full AMQP(S) address you want to receive from, e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/mytopic/Subscriptions/mysub.
  * The callback takes an error, a message payload, and any annotations on the message, and is called every time a message
    is received.
* `eventHubSend(uri, payload, [partitionKey], cb)`
  * The URI should be the full AMQPS address of the hub with included SAS name and key,
    e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/myeventhub
  * The payload is a JSON payload (which might get `JSON.stringify`'d), or a string.
  * The (optional) partition key is a string that gets set as the partition key for the message (delivered in the
    message annotations).
  * The callback takes an error, and is called when the message is sent.
* `eventHubReceive(uri, [offset], cb)`
  * The URI should be the AMQPS address of the hub with included SAS name and key, consumer group suffix and partition,
    e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/myeventhub/ConsumerGroups/_groupname_/Partition/_partition_
  * The (optional) offset should be the string offset provided by the message annotations of received messages, allowing
    connections to pick up receipt from where they left off.
  * The callback takes an error, a partition ID, a message payload, and any annotations on the message, and is called every time a message
    is received.
* `disconnect(uri, cb)`
  * If a URI is not provided, it will disconnect from all open links and tear down the connection.  If one is provided, it
    should tear down only those links matching the URI.  If multiple links match the same URI (e.g. sending/receiving to the same
    queue), they will all get torn down.  If all links are torn down as a consequence, we will tear down the connection as well.

Any class implementing these five methods is duck-type compatible with `node-sbus` and can be used.
