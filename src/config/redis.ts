import { Redis } from "ioredis";
import { env } from "./env.js";

if (!env.redisUrl) {
  throw new Error('❌ REDIS_URL not defined');
}

const redisConnection = new Redis(env.redisUrl, {

  connectTimeout: 10_000,
  maxRetriesPerRequest: null,

  // Exponential backoff (prevents spam)
  retryStrategy(times) {
    if (times > 8) return null;
    return Math.min(1000 * 2 ** times, 30_000);
  },

  // Retry only for real network/DNS failures
  reconnectOnError(err) {
    const nodeErr = err as NodeJS.ErrnoException;
    return nodeErr?.code === 'ENOTFOUND' || nodeErr?.code === 'ECONNREFUSED' || nodeErr?.code === 'ETIMEDOUT';
  },
});

redisConnection.on('connect', () => {
  console.log('✅ Connected to Redis');
});

redisConnection.on('error', err => {
  console.error('❌ Redis connection error:', err);
});

export const redis = redisConnection;
