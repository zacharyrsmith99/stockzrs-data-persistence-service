import dataPersitenceFactory from "./kafka/kafkaPersistenceFactory";
import BaseLogger from "./utils/logger";
import path from "path";
import { Pool } from "pg";

async function start() {
  const logger = new BaseLogger(path.join(__dirname, "app.log"));
  const postgresConfig = {
    user: process.env.POSTGRES_USERNAME,
    host: process.env.POSTGRES_HOST,
    database: process.env.POSTGRES_DB_NAME,
    password: process.env.POSTGRES_PASSWORD,
    port: parseInt(process.env.POSTGRES_PORT || "5432"),
    ssl: {
      rejectUnauthorized: false,
    },
  };
  const pool = new Pool(postgresConfig);

  try {
    await dataPersitenceFactory(logger, pool);
    logger.info("Data Persistence Service started successfully");
  } catch (error) {
    logger.error(`Failed to start Data Persistence Service: ${error}`);
    process.exit(1);
  }
}

start();
