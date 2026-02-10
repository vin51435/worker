import { publishEvent } from "#src/events/publisher.js";
import { Answer } from "#src/models/Answer.js";
import { runAI } from "#src/processors/ai.processor.js";
import { scoreAnswer } from "#src/processors/scoring.processor.js";
import { runSTT } from "#src/processors/stt.processor.js";


export async function evaluateAnswerJob(data: { answerId: string }) {
  const answer = await Answer.findById(data.answerId);
  if (!answer) throw new Error('Answer not found');

  const transcript = await runSTT(answer.audioUrl);
  const aiResult = await runAI(transcript);
  const score = scoreAnswer(aiResult);

  await Answer.updateOne(
    { _id: answer._id },
    {
      transcript,
      score,
      feedback: aiResult.feedback,
      status: 'done',
    }
  );

  await publishEvent('answer.evaluated', {
    answerId: answer._id,
    score,
  });
}
