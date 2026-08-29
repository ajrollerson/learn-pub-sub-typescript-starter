import amqp from "amqplib";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerMove, handlerPause } from "./handlers.js";
import { publishJSON } from "../internal/pubsub/publish.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection successful!")
  console.log("Starting Peril client...");

  try {
    const username = await clientWelcome();
    await declareAndBind(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient);
    const gameState = new GameState(username);
    const newChannel = await conn.createConfirmChannel()
    await subscribeJSON(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState));
    await subscribeJSON(conn, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, `${ArmyMovesPrefix}.*`, SimpleQueueType.Transient, handlerMove(gameState));

    process.on("SIGINT", async () => {
    console.log("Programme shutting down!");
    await conn.close();
    process.exit(0);
    });


    while (true) {
      const input = await getInput()

      if (input.length === 0) {
        continue;
      }
      const command = input[0];
      if (command === "spawn") {
        try {
            commandSpawn(gameState, input);
          } catch (err) {
            if (err instanceof Error) {
              console.error(err.message);
            } else {
              console.error("An unexpected error occurred:", err);
            }
          }
      } else if (command === "move") {
        try {
            const move = commandMove(gameState, input);
            await publishJSON(newChannel, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, move)
          } catch (err) {
            if (err instanceof Error) {
              console.error(err.message);
            } else {
              console.error("An unexpected error occurred:", err);
            }
          }
      } else if (command === "status") {
        await commandStatus(gameState);
      } else if (command === "help") {
        printClientHelp()
      } else if (command ==="spam") {
        console.log("Spamming not allowed yet!");
      } else if (command === "quit") {
        printQuit();
        await conn.close();
        process.exit(0);
      } else {
        console.error("Command unknown! Try again");
        continue;
      }
    }

  } catch (err) {
    console.log(err);
  }

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
