const { Kafka } = require('kafkajs')
const avro = require("avsc");


module.exports = () => {
  const type = avro.Type.forSchema({
    type: "record",
    fields: [
      { name: "userId", type: "string" },
      { name: "productId", type: "string" },
      { name: "status", type: "string" },
      { name: "price", type: "int" },
      { name: "creditCard", type: "string" },
      { name: "_id", type: "string" },

    ],
  });

  const kafka = new Kafka({
    brokers: ['my-cluster-kafka-brokers:9092'],
    retry: {
      initialRetryTime: 100,
      retries: 8
    }

  })

  const consumer = kafka.consumer({ groupId: 'group2' })

  const run = async () => {

    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic: 'high-trans', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          partition,
          offset: message.offset,
          value: message.value.toString(),
        });
        // const decodedValue = type.fromBuffer(message.value);
        // console.log(decodedValue);

      },
    })
  }

  run().catch(console.error)
}
