import mongoose from 'mongoose';
import { env } from './env.js';

mongoose.connect(env.mongoUri);

mongoose.connection.on('connected', () => {
  console.log('🟢 MongoDB connected');
});
