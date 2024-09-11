import BaseLogger from "../utils/logger";
import DataPersistenceService from "./kafkaPersistence";
import { Kafka } from "kafkajs";
import { Pool } from 'pg';

const MAX_RETRIES = 5;
const INITIAL_RETRY_DELAY = 5000;

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  logger: BaseLogger,
  retries = MAX_RETRIES,
  delay = INITIAL_RETRY_DELAY,
): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (retries > 0) {
      logger.warn(
        `Operation failed. Retrying in ${delay}ms. Retries left: ${retries}`,
      );
      await sleep(delay);
      return retryWithBackoff(operation, logger, retries - 1, delay * 2);
    } else {
      logger.error("Max retries reached. Throwing error.");
      throw error;
    }
  }
}

export default async function dataPersitenceFactory(logger: BaseLogger, pool: Pool) {
  const brokers = process.env.KAFKA_BOOTSTRAP_SERVERS
    ? process.env.KAFKA_BOOTSTRAP_SERVERS!.split(",")
    : ["localhost:9092"];

  const kafka = new Kafka({
    clientId: "stockzrs-data-persistence-service",
    brokers: brokers,
    // sasl: {
    //   mechanism: "scram-sha-512",
    //   username: process.env.KAFKA_USERNAME!,
    //   password: process.env.KAFKA_PASSWORD!,
    // },
    connectionTimeout: 3000,
    retry: {
      initialRetryTime: 100,
      retries: 8,
    },
  });

  const dataPersistenceService = new DataPersistenceService(kafka, pool, logger);

  await retryWithBackoff(async () => {
    logger.info("Attempting to start Data Persistence Kafka...");
    await dataPersistenceService.start();
    logger.info("Data Persistence Kafka started successfully.");
  }, logger);

  return dataPersistenceService;
}
