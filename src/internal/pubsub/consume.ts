import amqp, { type Channel } from "amqplib";
import { buffer } from "stream/consumers";
import { XDeadLetterExchange } from "../routing/routing.js";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
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
  arguments: {
  "x-dead-letter-exchange": `${XDeadLetterExchange}`,
  }
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
  handler: (data: T) => AckType,
): Promise<void> {
    const [newChannel, newQueue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    newChannel.consume(newQueue.queue, function (msg: amqp.ConsumeMessage | null) {
        if (msg === null) {
            return;
        }
        const buffer = msg.content.toString();
        try {
            const parsedMsg = JSON.parse(buffer) as T;
            const ackType = handler(parsedMsg);
            switch (ackType) {
                case AckType.Ack:
                    newChannel.ack(msg);
                    console.log("Messaged acked!");
                    break;
                case AckType.NackRequeue:
                    newChannel.nack(msg, false, true);
                    console.log("Message Nackrequeued!");
                    break;
                case AckType.NackDiscard:
                    newChannel.nack(msg, false, false);
                    console.log("Message Nackdiscarded");
                    break;
            }
        } catch (err) {
            console.error(err)
        }
    });
}
