import mongoose from 'mongoose';
import { env } from './config/env.js';

mongoose.connect(env.mongoUri);

mongoose.connection.on('connected', () => {
  console.log('🟢 MongoDB connected');
});
