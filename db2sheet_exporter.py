import os
import sqlite3
import argparse
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ── 環境変数（直書き禁止） ──────────────────────────────────────────
DB_PATH = os.getenv("DB2SHEET_DB_PATH", os.path.abspath("./market_data.db"))
GSHEET_NAME = os.getenv("DB2SHEET_GSHEET_NAME", "PortfolioData")
GSHEET_TAB = os.getenv("DB2SHEET_GSHEET_TAB", "Export")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # 必須

# テーブル名（既存スキーマ互換・匿名化対応）
T_CONSENSUS = os.getenv("DB2SHEET_T_CONSENSUS", "consensus_url")
T_FUND      = os.getenv("DB2SHEET_T_FUND", "nikkei_reports")      # 例: 基本ファクト類
T_SCORE     = os.getenv("DB2SHEET_T_SCORE", "moneyworld_reports") # 例: スコア/カテゴリ類
T_RATIO     = os.getenv("DB2SHEET_T_RATIO", "sbi_reports")        # 例: 比率・成長類

# 英語列名 → 日本語列名（シンプルな例）
COLUMN_NAME_MAP = {
    "price": "株価",
    "per": "予想PER",
    "yield_rate": "予想配当利回り",
    "pbr": "PBR（実績）",
    "roe_n": "ROE（予想）",
    "earning_yield": "株式益回り（予想）",
    "sales_growth": "増収率",
    "op_profit_growth": "経常増益率",
    "op_margin": "売上高経常利益率",
    "roe_s": "ROE",
    "roa": "ROA",
    "equity_ratio": "株主資本比率",
    "dividend_payout": "配当性向",
    "rating": "レーティング",
    "sales": "売上高予想",
    "profit": "経常利益予想",
    "scale": "規模",
    "cheap": "割安度",
    "growth": "成長",
    "profitab": "収益性",
    "safety": "安全性",
    "risk": "リスク",
    "return_rate": "リターン",
    "liquidity": "流動性",
    "trend": "トレンド",
    "forex": "為替",
    "technical": "テクニカル",
}

def parse_date_range(date_str: str) -> Tuple[datetime.date, datetime.date]:
    # "YYYYMMDD-YYYYMMDD" or "YYYYMMDD-MMDD" or single "YYYYMMDD"
    if "-" in date_str:
        start_str, end_str = date_str.split("-")
        end_str = end_str.strip()
        if len(end_str) == 4:  # e.g. 0731 -> 20250731
            end_str = start_str[:4] + end_str
        start = datetime.strptime(start_str, "%Y%m%d").date()
        end = datetime.strptime(end_str, "%Y%m%d").date()
        return start, end
    d = datetime.strptime(date_str, "%Y%m%d").date()
    return d, d

def get_available_dates_in_range(start_date, end_date) -> List[str]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        f"""
        SELECT DISTINCT target_date
          FROM {T_CONSENSUS}
         WHERE target_date BETWEEN ? AND ?
        """,
        (start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")),
    ).fetchall()
    conn.close()
    return sorted([r[0] for r in rows])

def fetch_merged_data(dates: List[str]) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    placeholders = ",".join(["?"] * len(dates))
    query = f"""
        SELECT
            c.target_date, c.code, c.name,
            f.sector, f.price, f.per, f.yield_rate, f.pbr, f.roe AS roe_n,
            f.earning_yield,
            r.sales_growth, r.op_profit_growth, r.op_margin, r.roe AS roe_s,
            r.roa, r.equity_ratio, r.dividend_payout,
            s.rating, s.sales, s.profit, s.scale, s.cheap, s.growth, s.profitab,
            s.safety, s.risk, s.return_rate, s.liquidity, s.trend, s.forex, s.technical,
            c.link_a AS link_a, c.link_b AS link_b, c.link_c AS link_c
        FROM {T_CONSENSUS} c
        LEFT JOIN {T_FUND} f
               ON c.target_date = f.target_date AND c.code = f.code
        LEFT JOIN {T_SCORE} s
               ON c.target_date = s.target_date AND c.code = s.code
        LEFT JOIN {T_RATIO} r
               ON c.target_date = r.target_date AND c.code = r.code
        WHERE c.target_date IN ({placeholders})
    """
    df = pd.read_sql_query(query, conn, params=dates)
    conn.close()
    return df

def reshape_data(df: pd.DataFrame) -> pd.DataFrame:
    """縦持ち（target_dateごと）を横持ち化。"""
    need = {"target_date", "code", "name"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"必要列が不足しています: {sorted(miss)}")

    url_cols = [c for c in ["link_a", "link_b", "link_c"] if c in df.columns]
    url_df = (
        df.sort_values("target_date", ascending=False)
          .drop_duplicates(subset=["code"])
          .set_index("code")[url_cols] if url_cols else None
    )

    value_cols = [
        "price", "per", "yield_rate", "pbr", "roe_n", "earning_yield",
        "sales_growth", "op_profit_growth", "op_margin", "roe_s", "roa",
        "equity_ratio", "dividend_payout",
        "rating", "sales", "profit", "scale", "cheap", "growth", "profitab",
        "safety", "risk", "return_rate", "liquidity", "trend", "forex", "technical",
    ]

    keys = ["code", "name"]
    if "sector" in df.columns:
        keys.append("sector")

    all_dates = sorted(df["target_date"].astype(str).unique(), reverse=True)

    records = []
    for td in all_dates:
        sub = df[df["target_date"] == td].copy()
        drop_cols = ["target_date"] + url_cols
        sub = sub.drop(columns=[c for c in drop_cols if c in sub.columns], errors="ignore")
        rename_map = {c: f"{COLUMN_NAME_MAP.get(c, c)}_{td}" for c in value_cols if c in sub.columns}
        sub = sub.rename(columns=rename_map)
        keep_cols = [c for c in keys if c in sub.columns] + [c for c in sub.columns if c not in keys]
        sub = sub[keep_cols]
        records.append(sub)

    base_keys = df[[k for k in keys if k in df.columns]].drop_duplicates().copy()
    merged = base_keys
    for df_ in records:
        dup_cols = [c for c in df_.columns if c in merged.columns and c not in keys]
        if dup_cols:
            df_ = df_.drop(columns=dup_cols, errors="ignore")
        merged = pd.merge(merged, df_, on=[k for k in keys if k in df_.columns], how="outer")

    if url_df is not None:
        merged = merged.set_index("code").join(url_df, how="left").reset_index()

    ordered = [c for c in ["code", "name", "sector"] if c in merged.columns]
    for td in all_dates:
        for c in value_cols:
            col = f"{COLUMN_NAME_MAP.get(c, c)}_{td}"
            if col in merged.columns:
                ordered.append(col)
    ordered += [c for c in url_cols if c in merged.columns]
    merged = merged[ordered]
    merged = merged.rename(columns={"code": "コード", "name": "企業名", "sector": "セクター"} if "sector" in merged.columns else {"code": "コード", "name": "企業名"})
    merged.insert(0, "日付", ",".join(all_dates))
    return merged

def authorize_gspread():
    if not GOOGLE_CREDENTIALS or not os.path.exists(GOOGLE_CREDENTIALS):
        raise RuntimeError("環境変数 GOOGLE_APPLICATION_CREDENTIALS が未設定、またはファイルが存在しません。")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS, scopes=scope)
    return gspread.authorize(creds).open(GSHEET_NAME)

def sanitize(val):
    return "" if pd.isna(val) else str(val)

def export_to_gsheet(df: pd.DataFrame):
    header = list(df.columns)
    rows = [list(map(sanitize, row)) for _, row in df.iterrows()]
    sh = authorize_gspread()
    try:
        ws = sh.worksheet(GSHEET_TAB)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=GSHEET_TAB, rows="1000", cols="100")
    ws.clear()
    ws.update([header] + rows)
    print(f"✅ {len(df)}件のデータをGoogle Sheetsに出力しました。sheet='{GSHEET_NAME}/{GSHEET_TAB}'")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="例: 20250728 / 20250701-0731 / 20250701-20250731")
    args = ap.parse_args()

    start_date, end_date = parse_date_range(args.date)
    dates = get_available_dates_in_range(start_date, end_date)
    if not dates:
        print("⚠️ 指定範囲に利用可能な target_date が見つかりません。")
        return

    df = fetch_merged_data(dates)
    if df.empty:
        print("⚠️ データが見つかりませんでした。")
        return

    reshaped_df = reshape_data(df)
    export_to_gsheet(reshaped_df)

if __name__ == "__main__":
    main()
