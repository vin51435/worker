import rabbitConnection from "#src/config/rabbitmq.js";
import { ConsumerStatus } from "rabbitmq-client";

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 30000; // 30 seconds

async function startWorker() {
  const connection = await rabbitConnection();

  connection.createConsumer(
    {
      queue: "interview-worker.queue",

      queueOptions: {
        durable: true,
        arguments: {
          "x-dead-letter-exchange": "interview.retry.exchange"
        }
      },

      exchanges: [
        { exchange: "interview.event", type: "topic", durable: true },
        { exchange: "interview.retry.exchange", type: "direct", durable: true },
        { exchange: "interview.dlx", type: "direct", durable: true }
      ],

      queueBindings: [
        { exchange: "interview.event", routingKey: "interview.*" }
      ],

      qos: { prefetchCount: 5 }
    },

    async (msg) => {
    //   const retryCount = msg.properties.headers?.["x-retry-count"] ?? 0;
    const retryCount = msg.headers?.["x-retry-count"] ?? 0;

      try {
        const data = JSON.parse(msg.body.toString());

        await processJob(msg.routingKey, data);

        return ConsumerStatus.ACK;
      } catch (err) {
        console.error("Job failed:", err);

        if (retryCount < MAX_RETRIES) {
          await retryMessage(connection, msg, retryCount + 1);
        } else {
          await sendToDLQ(connection, msg);
        }

        return ConsumerStatus.ACK; // acknowledge original
      }
    }
  );

  // 🔁 Retry Queue
  await connection.createConsumer({
    queue: "interview-worker.retry",
    queueOptions: {
      durable: true,
      arguments: {
        "x-message-ttl": RETRY_DELAY_MS,
        "x-dead-letter-exchange": "interview.event"
      }
    },
    exchanges: [
      { exchange: "interview.retry.exchange", type: "direct", durable: true }
    ],
    queueBindings: [
      { exchange: "interview.retry.exchange", routingKey: "retry" }
    ]
  },
  async () => {
    // Do nothing.
    // Messages expire via TTL and auto dead-letter back.
  });

  // 💀 Dead Letter Queue
  await connection.createConsumer({
    queue: "interview-worker.dlq",
    queueOptions: { durable: true },
    exchanges: [
      { exchange: "interview.dlx", type: "direct", durable: true }
    ],
    queueBindings: [
      { exchange: "interview.dlx", routingKey: "failed" }
    ]
  },
  async (msg) => {
    console.error("DLQ Message:", msg.body.toString());
    return ConsumerStatus.ACK;
  });
}

async function retryMessage(connection: any, msg: any, retryCount: number) {
  const publisher = connection.createPublisher({ confirm: true });

  await publisher.send(
    {
      exchange: "interview.retry.exchange",
      routingKey: "retry"
    },
    msg.body,
    {
      headers: {
        "x-retry-count": retryCount
      }
    }
  );
}

async function sendToDLQ(connection: any, msg: any) {
  const publisher = connection.createPublisher({ confirm: true });

  await publisher.send(
    {
      exchange: "interview.dlx",
      routingKey: "failed"
    },
    msg.body,
    {
      headers: msg.properties.headers
    }
  );
}

async function processJob(routingKey: string, data: any) {
  if (routingKey === "interview.test") {
    console.log("Processing test job");
  }

  if (routingKey === "interview.evaluate") {
    console.log("Processing evaluate job");
  }
}

startWorker();
