//  workaround, The queues aren't being created by the publisher configuration. Let me try a more direct approach - we'll create a dummy consumer for each retry queue, let it set up the infrastructure, then close it:

import { redis } from "../config/redis.js";
import { evaluateAnswerJob } from "../jobs/evaluateAnswer.job.js";
import { testJob } from "#src/jobs/test.job.js";
import rabbitConnection from "#src/config/rabbitmq.js";
import { ConsumerStatus } from "rabbitmq-client";

const handlers: Record<string, (data: any) => Promise<void>> = {
  "interview.test": testJob,
  "interview.evaluate": evaluateAnswerJob,
};

const RETRY_DELAY_MS = 5000;
const MAX_RETRIES = 2;

interface MessageWrapper {
  retryCount: number;
  data: any;
}

async function setupInfrastructure(connection: any) {
  // Create exchanges first
  const exchangeSetup = connection.createConsumer(
    {
      queue: "temp-exchange-setup",
      queueOptions: { durable: false, autoDelete: true },
      exchanges: [
        { exchange: "interview.event", type: "topic", durable: true },
        { exchange: "interview.retry.exchange", type: "direct", durable: true },
        { exchange: "interview.dlx", type: "direct", durable: true },
      ],
    },
    async () => ConsumerStatus.ACK
  );

  await new Promise(resolve => setTimeout(resolve, 1000));

  // Create retry queue for test
  const retryTestSetup = connection.createConsumer(
    {
      queue: "interview-worker.retry.test",
      queueOptions: {
        durable: true,
        arguments: {
          "x-message-ttl": RETRY_DELAY_MS,
          "x-dead-letter-exchange": "interview.event",
          "x-dead-letter-routing-key": "interview.test",
        },
      },
      exchanges: [
        { exchange: "interview.retry.exchange", type: "direct", durable: true },
      ],
      queueBindings: [
        {
          queue: "interview-worker.retry.test",
          exchange: "interview.retry.exchange",
          routingKey: "retry.test",
        },
      ],
    },
    async () => ConsumerStatus.ACK
  );

  // Create retry queue for evaluate
  const retryEvaluateSetup = connection.createConsumer(
    {
      queue: "interview-worker.retry.evaluate",
      queueOptions: {
        durable: true,
        arguments: {
          "x-message-ttl": RETRY_DELAY_MS,
          "x-dead-letter-exchange": "interview.event",
          "x-dead-letter-routing-key": "interview.evaluate",
        },
      },
      exchanges: [
        { exchange: "interview.retry.exchange", type: "direct", durable: true },
      ],
      queueBindings: [
        {
          queue: "interview-worker.retry.evaluate",
          exchange: "interview.retry.exchange",
          routingKey: "retry.evaluate",
        },
      ],
    },
    async () => ConsumerStatus.ACK
  );

  await new Promise(resolve => setTimeout(resolve, 1000));

  // Close all setup consumers
  await exchangeSetup.close();
  await retryTestSetup.close();
  await retryEvaluateSetup.close();

  console.log("✅ Retry infrastructure created");
}

async function startWorker() {
  const connection = await rabbitConnection();

  // Setup all infrastructure first
  await setupInfrastructure(connection);

  // Create the actual publisher for sending messages
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
        const messageBody = JSON.parse(msg.body.toString());
        
        let retryCount: number;
        let data: any;

        if (typeof messageBody === 'object' && 'retryCount' in messageBody && 'data' in messageBody) {
          retryCount = messageBody.retryCount;
          data = messageBody.data;
        } else {
          retryCount = 0;
          data = messageBody;
        }

        console.log(
          `[${new Date().toISOString()}] Received message ${msg.routingKey} with retry count: ${retryCount}`
        );

        try {
          await processJob(msg.routingKey, data);
          console.log(`[${new Date().toISOString()}] Job completed successfully`);
          return ConsumerStatus.ACK;
        } catch (err: any) {
          console.error(`[${new Date().toISOString()}] Job failed:`, err.message);

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
    }
  );

  // Dead Letter Queue consumer
  connection.createConsumer(
    {
      queue: "interview-worker.dlq",
      queueOptions: { durable: true },
      exchanges: [{ exchange: "interview.dlx", type: "direct", durable: true }],
      queueBindings: [{ exchange: "interview.dlx", routingKey: "failed" }],
    },
    async (msg) => {
      console.error(`[${new Date().toISOString()}] DLQ Message:`, {
        routingKey: msg.routingKey,
        body: msg.body.toString(),
      });
      return ConsumerStatus.ACK;
    }
  );

  console.log("✅ Interview Worker started");
}

async function retryMessage(
  publisher: any,
  routingKey: string,
  data: any,
  retryCount: number
) {
  const retryRoutingKey = routingKey.replace("interview.", "retry.");
  
  console.log(
    `[${new Date().toISOString()}] Retrying message (attempt ${retryCount}/${MAX_RETRIES}) - will retry in ${RETRY_DELAY_MS}ms`
  );

  try {
    const messageWrapper: MessageWrapper = {
      retryCount,
      data,
    };

    await publisher.send(
      {
        exchange: "interview.retry.exchange",
        routingKey: retryRoutingKey,
      },
      Buffer.from(JSON.stringify(messageWrapper))
    );
    
    console.log(
      `[${new Date().toISOString()}] Message sent to retry queue: ${retryRoutingKey}`
    );
  } catch (error) {
    console.error("Failed to send message to retry queue:", error);
  }
}

async function sendToDLQ(
  publisher: any,
  routingKey: string,
  data: any,
  retryCount: number
) {
  console.log(`[${new Date().toISOString()}] Max retries exceeded, sending to DLQ`);

  try {
    const messageWrapper = {
      retryCount,
      data,
      originalRoutingKey: routingKey,
      failureReason: "max-retries-exceeded",
      failedAt: new Date().toISOString(),
    };

    await publisher.send(
      {
        exchange: "interview.dlx",
        routingKey: "failed",
      },
      Buffer.from(JSON.stringify(messageWrapper))
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