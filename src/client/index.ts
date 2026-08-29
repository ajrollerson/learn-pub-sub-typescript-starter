import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { clientWelcome, getInput } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { printServerHelp } from "../internal/gamelogic/gamelogic.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const confirmChannel = await conn.createConfirmChannel()
  console.log("Connection successful!")
  console.log("Starting Peril client...");

  try {
    const username = await clientWelcome();
    await declareAndBind(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient);
    printServerHelp()

    while (true) {
      const input = await getInput()

      if (input.length === 0) {
        continue;
      }
      const command = input[0];
      if (command === "pause") {
        try {
          console.log("Pausing!");
            await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: true });
          } catch (err) {
            console.log(err);
          }
      } else if (command === "resume") {
        try {
            console.log("Resuming!");
            await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: false });
          } catch (err) {
            console.log(err);
          }
      } else if (command === "quit") {
        console.log("Thanks for playing! Exiting...")
        break;
      } else {
        console.log("Sorry, I don't understand. Please write again.")
      }
    }

  } catch (err) {
    console.log(err);
  }

clientWelcome

  process.on("SIGINT", async () => {
    console.log("Programme shutting down!");
    await conn.close();
    process.exit(0);
 });

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
