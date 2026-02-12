import { Connection } from 'rabbitmq-client';
import { env } from './env.js';

async function createConnection() {
  const rabbit = new Connection({
    url: env.rabbitmqUrl,
  });
  rabbit.on("error", (err) => {
    console.log("RabbitMQ connection error", err);
  });
  rabbit.on("connection", () => {
    console.log("Connection successfully (re)established");
  });

  return rabbit;
}

export default createConnection 