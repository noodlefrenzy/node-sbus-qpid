var Qpid            = require('node-qpid'),
    Builder         = require('node-amqp-encoder').Builder,
    Sbus            = require('node-sbus');

// takes a message generated from node-qpid and returns a massaged/cleaned up message
function parseMessageFromQpid(message) {
    // node-qpid returns everything as a string currently
    // and that string has added quotes.  This removes those quotes.
    var body = message.body.substr(1, message.body.length - 2);

    var result = body;
    try {
        result = JSON.parse(body);
    } catch (e) {
        result = body; // Failed to parse, revert.
    }
    return result;
}

function generatePartitionKeyAnnotation(key) {
    var annotations = null;
    if(key) {
        var b = new Builder();
        annotations = b.map().
            symbol("x-opt-partition-key").
            string(key).
            end().
            encode();
    }
    return annotations;
}

function generateOffsetFilter(offset) {
    var filter = null;
    if (offset) {
        var b = new Builder();
        filter = b.map().
            symbol("apache.org:selector-filter:string").
            described().symbol("apache.org:selector-filter:string").
            string("amqp.annotation.x-opt-offset > '" + offset + "'").
            end().
            encode();
    }
    return filter;
}

function SbusAdapter() {
    this.messenger = new qpid.proton.Messenger();
}

SbusAdapter.prototype.send = function(uri, payload, cb) {
    throw new Error('Not yet implemented.');
};

SbusAdapter.prototype.receive = function(uri, cb) {
    throw new Error('Not yet implemented.');
};

SbusAdapter.prototype.eventHubSend = function(uri, payload, partitionKey, cb){
    if (cb === undefined) {
        cb = partitionKey;
        partitionKey = undefined;
    }

    var message = payload;

    if(typeof message !== 'string') {
        message = JSON.stringify(message);
    }

    var annotations;
    if(partitionKey) {
        annotations = generatePartitionKeyAnnotation(partitionKey);
    }

    this.messenger.send({
        address: uri,
        annotations: annotations,
        body: message },
        function(err) {
            if(callback) {
                callback(err);
            }
        });
};

SbusAdapter.prototype.eventHubReceive = function(uri, offset, cb) {
    if (cb === undefined) {
        cb = offset;
        offset = undefined;
    }

    var self = this;
    if (!this.subscriptions) {
        this.subscriptions = {};
        // Register the message handler to allocate to appropriate subscription
        this.messenger.on('message', function(message, subscription) {
            var cbForSub = self.subscriptions[subscription];
            if (!cbForSub) {
                // Should we throw?  For now, just ignore.
            } else {
                var payload = parseMessageFromQpid(message);
                var partitionId = subscription.substring(subscription.lastIndexOf("/") + 1);
                var annotations;
                if (message.annotations && (message.annotations.length == 2)) {
                    annotations = {};
                    var annotationArray = message.annotations[1];
                    for (var i = 0; i < annotationArray.length / 2; i++) {
                        annotations[annotationsArray[2 * i][1]] = annotations[2 * i + 1][1];
                    }
                }
                cbForSub(null, partitionId, payload, annotations);
            }
        });

        this.messenger.receive();
    }

    if (!this.subscriptions[uri]) {
        var filter = generateOffsetFilter(offset);
        this.messenger.subscribe(uri, {sourceFilter: filter }, function (result) {
            self.subscriptions[uri] = cb;
        });
    }
};

SbusAdapter.prototype.disconnect = function(uri, cb) {

};

SbusAdapter.EventHubClient = function() {
    return new SbusAdapter();
};

SbusAdapter.ServiceBusClient = function() {
    return new SbusAdapter();
};

module.exports = SbusAdapter;
