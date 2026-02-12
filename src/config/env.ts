import { loadEnvFile } from 'node:process';
loadEnvFile('#src/../.env');

export const env = {
  redisUrl: process.env.REDIS_URL!,
  mongoUri: process.env.MONGO_URI!,
  rabbitmqUrl: process.env.RABBITMQ_URL!
};
