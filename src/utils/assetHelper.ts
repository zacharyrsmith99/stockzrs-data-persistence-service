import { DateTime } from "luxon";

export enum AssetTypeV2 {
  Cryptocurrency = "cryptocurrency",
  Index = "market_index",
  Currency = "currency",
  Stock = "stock",
  ETF = "etf",
}

export function getAssetTableTypeV2(assetTypeString: string): AssetTypeV2 {
  switch (assetTypeString) {
    case "CRYPTOCURRENCY":
      return AssetTypeV2.Cryptocurrency;
    case "INDEX":
      return AssetTypeV2.Index;
    case "CURRENCY":
      return AssetTypeV2.Currency;
    case "STOCK":
      return AssetTypeV2.Stock;
    case "ETF":
      return AssetTypeV2.ETF;
    default:
      throw new Error(`Unknown asset type: ${assetTypeString}`);
  }
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
      return "_pre_market";
    } else if (time >= "09:30" && time < "16:00") {
      return "_main_market";
    } else if (time >= "16:00") {
      return "_after_market";
    }
  }

  return "";
}

export function getAssetTableNameV2(assetType: AssetTypeV2): string {
  return `v2.${assetType.toLowerCase()}${getMarketSuffix(assetType)}_1min`;
}
