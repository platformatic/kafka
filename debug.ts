import { randomInt } from "node:crypto";
import {
  Admin,
  Consumer,
  type MessageToProduce,
  Producer,
} from "./src/index.ts";

const brokers = process.env.BROKERS || "127.0.0.1:9001";
const channels = ["channel-1", "channel-2", "channel-3"];
const messagesCount = process.env.MESSAGES ? parseInt(process.env.MESSAGES) : 100;
const batchSize = process.env.BATCH ? parseInt(process.env.BATCH) : messagesCount / 10;
const bootstrapBrokers = brokers.split(",");

const publish = true; // process.env.PUBLISH === "true" || process.env.PUBLISH === "1";

const topic = "bug-reproducer-topic" + Date.now();

const clientId = "bug-reproducer" + Date.now();
const groupId = "bug-reproducer-group" + Date.now();

const admin = new Admin({ bootstrapBrokers, clientId });
const producer = new Producer({ bootstrapBrokers, clientId });
const consumer = new Consumer({
  bootstrapBrokers,
  clientId,
  groupId,
});

// const topics = await admin.listTopics();
// if (topics.includes(topic)) {
//   console.log("Deleting topic");
//   await admin.deleteTopics({ topics: [topic] });
//   console.log("Topic deleted");
// }

try {
  console.log("Creating topic");
  await admin.createTopics({
    topics: [topic],
    partitions: 3,
  });
  console.log("Topic created");
} catch (e) {
  console.log("Topic already exists");
}
await admin.close();

if (publish) {
  console.log("Producing messages");
  const batch: MessageToProduce[] = [];
  for (let i = 0; i < messagesCount; i++) {
    const customer_id = 100_000*i
    const message = JSON.stringify({
      id: i,
      // transaction_id: i * 10_000_000,// randomInt(1_000_000, 10_000_000),
      // transaction_date: new Date().toISOString(),
      // customer_id: customer_id,
      // amount: 1_000*i,
      // channel: channels[randomInt(0, channels.length - 1)],
    });

    batch.push({
      key: Buffer.from("customer_id"),
      value: Buffer.from(message),
      topic: topic,
    });

    if (batch.length >= batchSize) {
      await producer.send({
        messages: batch,
      });

      batch.length = 0;
    }
  }
  if (batch.length > 0) {
    await producer.send({
      messages: batch,
    });
  }
  console.log("Publish DONE");
}

const stream = await consumer.consume({
  topics: [topic],
  autocommit: true,
  mode: "earliest",
  maxBytes: 1024
});

console.log("Consuming messages");
let i = 0;
for await (const message of stream) {
  console.log(message.value.toString());
  if (i % 10 === 0) {
    console.log(`Consumed ${i+1} messages`);
  }
  if (++i >= messagesCount) {
    break;
  }
}

console.log("Consume DONE");

await producer.close();
await consumer.close();

console.log("DONE");