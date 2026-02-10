import { rabbit } from '../config/rabbitmq.js';

let publisher: any;

export async function publishEvent(
  routingKey: string,
  payload: unknown
) {
  if (!publisher) {
    publisher = rabbit.createPublisher({
      confirm: true,
      exchanges: [
        {
          exchange: 'events',
          type: 'topic',
        },
      ],
    });
  }

  await publisher.send(
    {
      exchange: 'events',
      routingKey,
    },
    payload
  );
}
