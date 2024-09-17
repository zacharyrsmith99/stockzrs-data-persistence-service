import { DateTime } from "luxon";

export enum AssetTypeV2 {
  Cryptocurrency = "cryptocurrency",
  Index = "market_index",
  Currency = "currency",
  Stock = "stock",
  ETF = "etf",
}

function getMarketSuffix(assetType: AssetTypeV2): string {
  const now = DateTime.utc().setZone("America/New_York");
  const dayOfWeek = now.weekday;

  if (
    assetType === AssetTypeV2.Cryptocurrency ||
    assetType === AssetTypeV2.Currency
  ) {
    return "";
  }

  if (
    assetType === AssetTypeV2.Stock ||
    assetType === AssetTypeV2.ETF ||
    assetType === AssetTypeV2.Index
  ) {
    if (dayOfWeek < 1 || dayOfWeek > 5) {
      return "";
    }

    const time = now.toFormat("HH:mm");

    if (time < "09:30") {
      return "pre_market";
    } else if (time >= "09:30" && time < "16:00") {
      return "main_market";
    } else if (time >= "16:00") {
      return "after_market";
    }
  }

  return "";
}

export function getAssetTableNameV2(assetType: AssetTypeV2): string {
  return `${assetType.toLowerCase()}_${getMarketSuffix(assetType)}_1min`;
}
