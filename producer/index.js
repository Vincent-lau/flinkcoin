const ip = require('ip')

const { Kafka, CompressionTypes, logLevel } = require('kafkajs')
const WebSocket = require('ws');

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`],
  clientId: 'example-producer',
})

const topic = 'topic-test'
const producer = kafka.producer()


console.log('producer is running');


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
      console.log(msg);
      await producer.send({
        topic: topic,
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


// const getRandomNumber = () => Math.round(Math.random(10) * 1000)
// const createMessage = num => ({
//   key: `key-${num}`,
//   value: `value-${num}-${new Date().toISOString()}`,
// })

// const sendMessage = () => {
//   return producer
//     .send({
//       topic,
//       compression: CompressionTypes.GZIP,
//       messages: Array(getRandomNumber())
//         .fill()
//         .map(_ => createMessage(getRandomNumber())),
//     })
//     .then(console.log)
//     .catch(e => console.error(`[example/producer] ${e.message}`, e))
// }

// const run = async () => {
//   await producer.connect()
//   setInterval(sendMessage, 3000)
// }

// run().catch(e => console.error(`[example/producer] ${e.message}`, e))

// const errorTypes = ['unhandledRejection', 'uncaughtException']
// const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

// errorTypes.forEach(type => {
//   process.on(type, async () => {
//     try {
//       console.log(`process.on ${type}`)
//       await producer.disconnect()
//       process.exit(0)
//     } catch (_) {
//       process.exit(1)
//     }
//   })
// })

// signalTraps.forEach(type => {
//   process.once(type, async () => {
//     try {
//       await producer.disconnect()
//     } finally {
//       process.kill(process.pid, type)
//     }
//   })
// })


// // const consumer = kafka.consumer({ groupId: 'test-group' })


// // const send = async () => {
// //   // Producing
// //   await producer.connect()

// //   setInterval(() => {
// //     // Producing
// //     producer.send({
// //       topic: 'test-topic',
// //       messages: [
// //         { value: 'Hello KafkaJS user!' },
// //       ],
// //     })
// //   }, 3000);

// // }

// // send().catch(console.error)



// const run = async () => {
//   // Consuming
//   await consumer.connect()
//   await consumer.subscribe({ topic: 'coinbase', fromBeginning: true })

//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       console.log({
//         partition,
//         offset: message.offset,
//         value: message.value.toString(),
//       })
//     },
//   })
// }

// // run().catch(console.error)

