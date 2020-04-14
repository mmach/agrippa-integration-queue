var amqp = require('amqplib/callback_api');
var Crawler = require("crawler");
const redis = require('async-redis');
var Promise = require('bluebird');
var azure = require('azure-storage');

const CONN_URL = process.env.AMQP ? process.env.AMQP : 'amqp://oqxnmzzs:hUxy1BVED5mg9xWl8lvoxw3VAmKBOn7O@squid.rmq.cloudamqp.com/oqxnmzzs';
const PREFETCH = process.env.PREFETCH ? process.env.PREFETCH : 5;
const MAX_CONNECTIONS = process.env.MAX_CONNECTIONS ? process.env.MAX_CONNECTIONS : 10;
var queueSvc = azure.createQueueService();
queueSvc.messageEncoder = new azure.QueueMessageEncoder.TextBase64QueueMessageEncoder();

global.c = new Crawler({
    maxConnections: MAX_CONNECTIONS,
    retries: 30,
    retryTimeout: 60000,

});



// Queue just one URL, with default callback


// Queue some HTML code directly without grabbing (mostly for tests)
amqp.connect(CONN_URL, async function (error0, connection) {
    if (error0) {
        console.log(error0)
        throw error0;
    }

    connection.createChannel(function (error1, channel) {

        if (error1) {
            throw error1;
        }

        var queue = 'ready-to-import-queue';

        channel.prefetch(PREFETCH);

        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
        channel.consume(queue, async function (msg) {
            let arrayProductsGroup = []
            let arrayProduct = []
            let obj = msg.content.toString();
            obj = JSON.parse(obj);
            try {


                await new Promise((res, rej) => {
                    queueSvc.createMessage('crawler-product-queue', JSON.stringify(obj), function (error, results, response) {
                        if (!error) {
                            // Message inserted
                            res()
                        }
                        rej()
                    });
                })
                channel.ack(msg)
            } catch (err) {
                console.log(err);
                setTimeout(() => {
                    channel.nack(msg)

                }, 60000)

            }

            // console.log(arrayProduct);
            // channel.nack(msg);
            // console.log(" [x] Received %s", msg.content.toString());

        }, {
            noAck: false
        });
    });
});
