import rabbitConnection from "#src/config/rabbitmq.js";

let publisher: any;

async function initPublisher() {
  const connection = await rabbitConnection();

  publisher = connection.createPublisher({
    confirm: true,
    exchanges: [
      {
        exchange: "events",
        type: "topic",
      },
    ],
  });
}

async function publishEvent(routingKey: string, payload: unknown) {
  if (!publisher) {
    await initPublisher();
  }

  await publisher.send(
    {
      exchange: "events",
      routingKey,
    },
    payload,
  );
}

export { publishEvent };
