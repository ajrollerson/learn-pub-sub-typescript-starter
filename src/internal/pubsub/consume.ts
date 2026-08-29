import amqp, { type Channel } from "amqplib";
import { buffer } from "stream/consumers";

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

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {
    const [newChannel, newQueue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    newChannel.consume(newQueue.queue, function (msg: amqp.ConsumeMessage | null) {
        if (msg === null) {
            return;
        }
        const buffer = msg.content.toString();
        try {
            const parsedMsg = JSON.parse(buffer) as T;
            handler(parsedMsg);
            newChannel.ack(msg);
        } catch (err) {
            console.error(err)
        }
    });
}