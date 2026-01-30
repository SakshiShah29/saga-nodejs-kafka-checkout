import { Kafka, Admin } from "kafkajs";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(
  ","
);

const TOPICS = [
  {
    topic: "ORDER_SERVICE",
    numPartitions: 1,
    replicationFactor: 1,
  },
  {
    topic: "PAYMENT_SERVICE",
    numPartitions: 1,
    replicationFactor: 1,
  },
  {
    topic: "INVENTORY_SERVICE",
    numPartitions: 1,
    replicationFactor: 1,
  },
  {
    topic: "SERVICE_REPLY",
    numPartitions: 1,
    replicationFactor: 1,
  },
];

async function createTopics(): Promise<void> {
  const kafka = new Kafka({
    clientId: "saga-admin",
    brokers: KAFKA_BROKERS,
  });

  const admin: Admin = kafka.admin();

  try {
    console.log("Connecting to Kafka broker...");
    await admin.connect();
    console.log("Connected. Creating topics...");

    const existingTopics = await admin.listTopics();
    console.log("Existing topics:", existingTopics);

    const topicsToCreate = TOPICS.filter(
      (t) => !existingTopics.includes(t.topic)
    );
    if (topicsToCreate.length === 0) {
      console.log("All topics already exist. No topics to create.");
    } else {
      console.log(
        "Creating topics:",
        topicsToCreate.map((t) => t.topic)
      );
      await admin.createTopics({
        topics: topicsToCreate,
        waitForLeaders: true,
      });
      console.log("Topics created successfully.");
    }
    const allTopics = await admin.listTopics();
    console.log(" All topics:", allTopics);
  } catch (error) {
    console.error(" Error creating topics:", error);
    process.exit(1);
  } finally {
    await admin.disconnect();
    console.log("Disconnected from Kafka");
  }
}

createTopics()
  .then(() => {
    console.log("Topic setup complete!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });
