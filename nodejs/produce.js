var kafka = require('kafka-node');
Producer = kafka.Producer;
KeyedMessage = kafka.KeyedMessage;

console.log('Starting Kafka producer...');
const client = new kafka.KafkaClient({kafkaHost: 'com-dev-kafka-node1-vm.westeurope.cloudapp.azure.com:9092'});
var producer = new Producer(client);

var km = new KeyedMessage('key', 'message');
    
payloads = [
    { topic: 'TutorialTopic', messages: 'hi' }
        //{ topic: 'TutorialTopic', messages: 'hi', partition: 0 },
        //{ topic: 'topic2', messages: ['hello', 'world', km] }
 ];

producer.on('ready', function() {
    producer.send(payloads, function(err, data) {
        if(err) {
            console.log('send.err=' + err);
            return;            
        }
        console.log('data=' + JSON.stringify(data));
        console.log('Kafka producer done.');        
    });   
});

producer.on('error', function (err) { console.log('err=' + err);})