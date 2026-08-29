import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
const newChannel = await conn.createChannel();

const options = {
  durable: false,
  autoDelete: false,
  exclusive: false,
}

if (queueType === SimpleQueueType.Durable) {
    options.durable = true;
} 

if (queueType === SimpleQueueType.Transient) {
    options.autoDelete = true;
    options.exclusive = true;
}

const newQueue = await newChannel.assertQueue(queueName, options);
await newChannel.bindQueue(queueName, exchange, key);
return [newChannel, newQueue]
}