import { Schema, model } from 'mongoose';

const AnswerSchema = new Schema({
  audioUrl: String,
  transcript: String,
  score: Number,
  feedback: String,
  status: {
    type: String,
    enum: ['processing', 'done', 'failed'],
    default: 'processing'
  }
});

export const Answer = model('Answer', AnswerSchema);
