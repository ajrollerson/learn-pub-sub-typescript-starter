import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
    const buffer = Buffer.from(JSON.stringify(value));
    return new Promise((resolve, reject) => {
        ch.publish(
          exchange,
          routingKey, 
          buffer, 
          { contentType: "application/json" },
          (err) => {
            if (err) {
              reject(err);
              return;
            }
            resolve();
           } 
        );
    });
}