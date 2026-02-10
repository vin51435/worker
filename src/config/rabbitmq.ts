import { Connection } from 'rabbitmq-client';
import { env } from './env.js';

export const rabbit = new Connection({
  url: env.rabbitmqUrl,
});

rabbit.on('error', err => {
  console.error('RabbitMQ error', err);
});

rabbit.on('connection', () => {
  console.log('🟣 RabbitMQ connected');
});
