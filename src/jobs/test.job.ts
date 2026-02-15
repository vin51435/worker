import { publishEvent } from "#src/events/publisher.js";

export async function testJob(data: any) {
  console.log('testing',data);
  throw new Error('test error');
  
  // await new Promise(resolve => setTimeout(resolve, 10000)); // 10 seconds delay
  // await publishEvent('test', data);
}
