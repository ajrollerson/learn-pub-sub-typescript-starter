import amqp, { type Channel } from "amqplib";
import { buffer } from "stream/consumers";
import { XDeadLetterExchange } from "../routing/routing.js";
import { decode } from "@msgpack/msgpack";

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
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    await subscribe(conn, exchange, queueName, key, queueType, handler, (data) => JSON.parse(data.toString()));
};
            

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    await subscribe(conn, exchange, queueName, key, queueType, handler, (data) => decode(data) as T);
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
    const [newChannel, newQueue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);
    await newChannel.consume(newQueue.queue, async function (msg: amqp.ConsumeMessage | null) {
        if (msg === null) {
            return;
        }
        let decodedMsg: T;

        try {
            decodedMsg = deserializer(msg.content);
            } catch (err) {
             console.error("Error in decoding message!")
            return;
            }

        try {
            const ackType = await handler(decodedMsg);
            switch (ackType) {
                case AckType.Ack:
                    newChannel.ack(msg);
                    break;
                case AckType.NackRequeue:
                    newChannel.nack(msg, false, true);
                    break;
                case AckType.NackDiscard:
                    newChannel.nack(msg, false, false);
                    break;
                default: {
                    const unreachable: never = ackType;
                    console.error("Unexpected ack type:", unreachable);
                    return;
                }
            }
        } catch (err) {
            console.error(err);
            newChannel.nack(msg, false, false);
        }
    });
}
