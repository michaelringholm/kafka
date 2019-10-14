var kafka = require('kafka-node');
Consumer = kafka.Consumer;
KeyedMessage = kafka.KeyedMessage;

console.log('Starting Kafka consumer...');
const client = new kafka.KafkaClient({kafkaHost: 'com-dev-kafka-node1-vm.westeurope.cloudapp.azure.com:9092'});
var topic = [{ topic: 'TutorialTopic' }];
var options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: 'utf8',
    fromOffset: false
};

var consumer = new Consumer(client, topic, options);

 consumer.on('message', function(data) {
        console.log('data=' + JSON.stringify(data));
        console.log('Kafka consumer done.');
});

consumer.on('error', function (err) { console.log('err=' + err);})