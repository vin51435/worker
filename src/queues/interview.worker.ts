import { redis } from "../config/redis.js";
import { evaluateAnswerJob } from "../jobs/evaluateAnswer.job.js";
import { testJob } from "#src/jobs/test.job.js";
import rabbitConnection from "#src/config/rabbitmq.js";
import { ConsumerStatus, Publisher } from "rabbitmq-client";

const handlers: Record<string, (data: any) => Promise<void>> = {
  "interview.test": testJob,
  "interview.evaluate": evaluateAnswerJob,
};

const RETRY_DELAY_MS = 5000;
const MAX_RETRIES = 2;

async function startWorker() {
  const connection = await rabbitConnection();

  // Delete queues for testing
  await connection.queueDelete("interview-worker.retry.evaluate");
  await connection.queueDelete("interview-worker.queue");
  await connection.queueDelete("interview-worker.retry.test");

  // Declare exchanges
  await connection.exchangeDeclare({
    exchange: "interview.event",
    type: "topic",
    durable: true,
  });

  await connection.exchangeDeclare({
    exchange: "interview.retry.exchange",
    type: "direct",
    durable: true,
  });

  await connection.exchangeDeclare({
    exchange: "interview.dlx",
    type: "direct",
    durable: true,
  });

  // Declare retry queues
  await connection.queueDeclare({
    queue: "interview-worker.retry.test",
    durable: true,
    arguments: {
      "x-message-ttl": RETRY_DELAY_MS,
      "x-dead-letter-exchange": "interview.event",
      "x-dead-letter-routing-key": "interview.test",
    },
  });

  await connection.queueDeclare({
    queue: "interview-worker.retry.evaluate",
    durable: true,
    arguments: {
      "x-message-ttl": RETRY_DELAY_MS,
      "x-dead-letter-exchange": "interview.event",
      "x-dead-letter-routing-key": "interview.evaluate",
    },
  });

  // Bind retry queues to retry exchange
  await connection.queueBind({
    queue: "interview-worker.retry.test",
    exchange: "interview.retry.exchange",
    routingKey: "retry.test",
  });

  await connection.queueBind({
    queue: "interview-worker.retry.evaluate",
    exchange: "interview.retry.exchange",
    routingKey: "retry.evaluate",
  });

  console.log("✅ Retry infrastructure created");

  const publisher = connection.createPublisher({
    confirm: true,
  });

  // Main consumer
  connection.createConsumer(
    {
      queue: "interview-worker.queue",
      queueOptions: {
        durable: true,
      },
      exchanges: [
        { exchange: "interview.event", type: "topic", durable: true },
      ],
      queueBindings: [
        { exchange: "interview.event", routingKey: "interview.*" },
      ],
      qos: { prefetchCount: 5 },
    },
    async (msg) => {
      try {
        const retryCount = msg.headers?.["x-retry-count"] ?? 0;
        console.log(
          `[${new Date().toISOString()}] Received message ${msg.routingKey} with retry count: ${retryCount}`,
        );

        const data = JSON.parse(msg.body.toString());

        try {
          await processJob(msg.routingKey, data);
          console.log(
            `[${new Date().toISOString()}] Job completed successfully`,
          );
          return ConsumerStatus.ACK;
        } catch (err: any) {
          console.error(
            `[${new Date().toISOString()}] Job failed:`,
            err?.message || err,
          );

          if (retryCount < MAX_RETRIES) {
            await retryMessage(publisher, msg.routingKey, data, retryCount + 1);
          } else {
            await sendToDLQ(publisher, msg.routingKey, data, retryCount);
          }

          return ConsumerStatus.ACK;
        }
      } catch (parseError) {
        console.error("Failed to parse message:", parseError);
        return ConsumerStatus.DROP;
      }
    },
  );

  // Dead Letter Queue consumer
  // connection.createConsumer(
  //   {
  //     queue: "interview-worker.dlq",
  //     queueOptions: { durable: true },
  //     exchanges: [{ exchange: "interview.dlx", type: "direct", durable: true }],
  //     queueBindings: [{ exchange: "interview.dlx", routingKey: "failed" }],
  //   },
  //   async (msg) => {
  //     console.error(`[${new Date().toISOString()}] DLQ Message:`, {
  //       routingKey: msg.routingKey,
  //       retryCount: msg.headers?.["x-retry-count"],
  //       body: msg.body.toString(),
  //     });
  //     return ConsumerStatus.ACK;
  //   },
  // );

  console.log("✅ Interview Worker started");
}

async function retryMessage(
  publisher: Publisher,
  routingKey: string,
  data: any,
  retryCount: number,
) {
  const retryRoutingKey = routingKey.replace("interview.", "retry.");

  console.log(
    `[${new Date().toISOString()}] Retrying message (attempt ${retryCount}/${MAX_RETRIES}) - will retry in ${RETRY_DELAY_MS}ms`,
  );

  try {
    await publisher.send(
      {
        exchange: "interview.retry.exchange",
        routingKey: retryRoutingKey,
        headers: {
          "x-retry-count": retryCount,
        },
      },
      Buffer.from(JSON.stringify(data)),
    );

    console.log(
      `[${new Date().toISOString()}] Message sent to retry queue: ${retryRoutingKey}`,
    );
  } catch (error) {
    console.error("Failed to send message to retry queue:", error);
  }
}

async function sendToDLQ(
  publisher: Publisher,
  routingKey: string,
  data: any,
  retryCount: number,
) {
  console.log(
    `[${new Date().toISOString()}] Max retries exceeded, sending to DLQ`,
  );

  try {
    const dlqMessage = {
      data,
      originalRoutingKey: routingKey,
      failureReason: "max-retries-exceeded",
      failedAt: new Date().toISOString(),
    };

    await publisher.send(
      {
        exchange: "interview.dlx",
        routingKey: "failed",
        headers: {
          "x-retry-count": retryCount,
        },
      },
      Buffer.from(JSON.stringify(dlqMessage)),
    );

    console.log(`[${new Date().toISOString()}] Message sent to DLQ`);
  } catch (error) {
    console.error("Failed to send message to DLQ:", error);
  }
}

async function processJob(routingKey: string, data: any) {
  const handler = handlers[routingKey];

  if (!handler) {
    throw new Error(`Unknown job type: ${routingKey}`);
  }

  await handler(data);
}

startWorker();
