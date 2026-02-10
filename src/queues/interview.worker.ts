import { Worker } from 'bullmq';
import { redis } from '../config/redis.js';
import { evaluateAnswerJob } from '../jobs/evaluateAnswer.job.js';

new Worker(
  'interview',
  async job => {
    if (job.name === 'evaluate-answer') {
      await evaluateAnswerJob(job.data);
    }
  },
  {
    connection: redis,
    concurrency: 2
  }
);
