export function scoreAnswer(ai: {
  correctness: number;
  clarity: number;
  depth: number;
}) {
  return Math.round(
    (ai.correctness + ai.clarity + ai.depth) / 3
  );
}
