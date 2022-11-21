const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'producer',
  brokers: ['kafka:9092']
  // brokers: [process.env.KAFKA_BROKER]
});

const producer = kafka.producer();


console.log('producer is running');
const consumer = kafka.consumer({ groupId: 'test-group' })


// const send = async () => {
//   // Producing
//   await producer.connect()

//   setInterval(() => {
//     // Producing
//     producer.send({
//       topic: 'test-topic',
//       messages: [
//         { value: 'Hello KafkaJS user!' },
//       ],
//     })
//   }, 3000);

// }

// send().catch(console.error)

const run = async () => {
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'coinbase', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}

run().catch(console.error)

async function setup() {
  try {
    await producer.connect();
    const ws = new WebSocket('wss://ws-feed.exchange.coinbase.com');

    await new Promise(resolve => ws.once('open', resolve));

    // const products = process.env.PRODUCTS.split(',');

    ws.send(JSON.stringify({
      "type": "subscribe",
      "product_ids": [
        // ...products
        "ETH-BTC",
        "ETH-USD"
      ],
      "channels": [
        "level2",
        "heartbeat",
        {
          "name": "ticker",
          "product_ids": [
            // ...products
            "ETH-BTC",
            "ETH-USD"
          ]
        }
      ]
    }));

    ws.on('message', async msg => {
      await producer.send({
        // topic: process.env.KAFKA_TOPIC,
        topic: 'coinbase',
        messages: [
          {
            value: msg
          }
        ]
      });
    });

  } catch (err) {
    console.error(err);
  }
}

setup();

