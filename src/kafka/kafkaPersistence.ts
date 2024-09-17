import { Kafka, Consumer } from "kafkajs";
import { Pool } from "pg";
import BaseLogger from "../utils/logger";
import { DateTime } from "luxon";
import { AssetTypeV2, getAssetTableNameV2 } from "../utils/assetHelper";

enum AssetType {
  Cryptocurrency = "minute_by_minute_cryptocurrency",
  Index = "minute_by_minute_market_index",
  Currency = "minute_by_minute_currency",
  Stock = "minute_by_minute_stock",
  ETF = "minute_by_minute_etf",
}

interface AggregatedData {
  symbol: string;
  openTimestamp: number;
  closeTimestamp: number;
  type: string;
  openPrice: number;
  highPrice: number;
  lowPrice: number;
  closePrice: number;
}

export default class DataPersistenceService {
  private consumer: Consumer;
  private pool: Pool;
  private logger: BaseLogger;

  constructor(kafka: Kafka, pool: Pool, logger: BaseLogger) {
    this.consumer = kafka.consumer({ groupId: "data-persistence-service" });
    this.pool = pool;
    this.logger = logger;
  }

  private generateUpsertData(data: AggregatedData) {
    const dateTime = DateTime.fromSeconds(data.openTimestamp).toFormat(
      "yyyy-MM-dd HH:mm",
    );

    return [
      data.symbol,
      dateTime,
      data.openPrice,
      data.highPrice,
      data.lowPrice,
      data.closePrice,
    ];
  }

  private async upsertData(
    assetType: string,
    data: AggregatedData,
  ): Promise<void> {
    const query = `
      INSERT INTO ${assetType} (symbol, timestamp, open_price, high_price, low_price, close_price)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (symbol, timestamp)
      DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price
    `;

    const values = this.generateUpsertData(data);

    try {
      await this.pool.query(query, values);
      this.logger.info(
        `Upserted data for symbol: (${data.symbol}), asset type: (${assetType})`,
      );
    } catch (error) {
      this.logger.error(
        `Error upserting data values (${values}) for symbol (${data.symbol}), asset type: (${assetType}): (${error})`,
      );
      throw error;
    }
  }

  private async processMessage(value: Buffer) {
    const data: AggregatedData = JSON.parse(value.toString());
    try {
      const assetType = this.getAssetTableName(data.type);
      await this.upsertData(assetType, data);
    } catch (error) {
      this.logger.error(
        `Error processing aggregated data message message: ${error}`,
      );
    }
  }

  private async processMessageV2(value: Buffer) {
    const data: AggregatedData = JSON.parse(value.toString());
    try {
      const assetType = getAssetTableNameV2(data.type as AssetTypeV2);
      await this.upsertData(assetType, data);
    } catch (error) {
      this.logger.error(
        `Error processing aggregated data message message: ${error}`,
      );
    }
  }

  public async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: "minute-aggregated-financial-updates",
      fromBeginning: false,
    });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        if (message.value) {
          this.processMessage(message.value);
          this.processMessageV2(message.value);
        }
      },
    });
  }

  private getAssetTableName(assetTypeString: string): AssetType {
    switch (assetTypeString) {
      case "CRYPTOCURRENCY":
        return AssetType.Cryptocurrency;
      case "INDEX":
        return AssetType.Index;
      case "CURRENCY":
        return AssetType.Currency;
      case "STOCK":
        return AssetType.Stock;
      case "ETF":
        return AssetType.ETF;
      default:
        throw new Error(`Unknown asset type: ${assetTypeString}`);
    }
  }

  public async stop(): Promise<void> {
    await this.consumer.disconnect();
    await this.pool.end();
  }
}
