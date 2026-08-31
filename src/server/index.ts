import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { AckType, declareAndBind, SimpleQueueType, subscribeMsgPack } from "../internal/pubsub/consume.js";
import { writeLog } from "../internal/gamelogic/logs.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection successful!");
  console.log("Starting Peril server...");

  process.on("SIGINT", async () => {
    console.log("Programme shutting down!");
    await conn.close();
    process.exit(0);
  });

  const confirmChannel = await conn.createConfirmChannel();
  await declareAndBind(conn, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable);
  
  await subscribeMsgPack(conn, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable, async (data: GameLog) => {
  await writeLog(data);
  console.log("> ");
  return AckType.Ack;
  });

  while (true) {
    const input = await getInput();
    if (input.length === 0) {
      continue;
    }
    const command = input[0];

    if (command === "pause") {
      try {
        console.log("Pausing!");
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: true,
        });
      } catch (err) {
        console.error(err);
      }
    } else if (command === "resume") {
      try {
        console.log("Resuming!");
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: false,
        });
      } catch (err) {
        console.error(err);
      }
    } else if (command === "quit") {
      console.log("Exiting...");
      await conn.close();
      process.exit(0);
    } else {
      console.log("Sorry, I don't understand. Please write again.");
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});