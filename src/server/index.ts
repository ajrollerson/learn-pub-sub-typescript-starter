import amqp from "amqplib";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection successful!")
  console.log("Starting Peril server...");

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
