import os
import sqlite3
import argparse
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ====== 環境変数優先の設定 ======
DB_PATH = os.getenv("MARKET_DB_PATH", os.path.abspath("./market_data.db"))
CREDENTIALS_FILE = os.getenv("GSPREAD_CREDENTIALS", os.path.abspath("./credentials.json"))
SPREADSHEET_NAME = os.getenv("GSPREAD_SPREADSHEET", "MarketData")
SHEET_NAME = os.getenv("GSPREAD_SHEET_NAME", "StockData_統合")  # 既存GASとの整合のためデフォルトを維持


# ====== 英語列名 → 日本語列名（nikkei & moneyworld のみ。sbi_reports 由来は含めない）======
COLUMN_NAME_MAP = {
    # nikkei_reports 由来
    "price": "株価",
    "per": "予想PER",
    "yield_rate": "予想配当利回り",
    "pbr": "PBR（実績）",
    "roe_n": "ROE（予想）",
    "earning_yield": "株式益回り（予想）",

    # moneyworld_reports 由来
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


# ====== Google Sheets 認証 ======
def authorize_gspread():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(
            f"認証ファイルが見つかりません: {CREDENTIALS_FILE}\n"
            "環境変数 GSPREAD_CREDENTIALS でパスを指定してください。"
        )
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    gc = gspread.authorize(creds)
    return gc.open(SPREADSHEET_NAME)


# ====== 日付パース ======
def parse_date_range(date_str: str) -> Tuple[datetime.date, datetime.date]:
    """
    "YYYYMMDD-YYYYMMDD" / "YYYYMMDD- MMDD" / "YYYYMMDD" を許容
    """
    date_str = date_str.strip()
    if "-" in date_str:
        start_str, end_str = date_str.split("-")
        start_str = start_str.strip()
        end_str = end_str.strip()
        if len(end_str) == 4:  # 例: 0731 → 20250731
            end_str = start_str[:4] + end_str
        start = datetime.strptime(start_str, "%Y%m%d").date()
        end = datetime.strptime(end_str, "%Y%m%d").date()
        return start, end
    d = datetime.strptime(date_str, "%Y%m%d").date()
    return d, d


# ====== 利用可能日付の列挙 ======
def get_available_dates_in_range(start_date, end_date) -> List[str]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT DISTINCT target_date
        FROM consensus_url
        WHERE target_date BETWEEN ? AND ?
        """,
        (start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")),
    ).fetchall()
    conn.close()
    return sorted([r[0] for r in rows])


# ====== データ取得（sbi_reports 非マージ）======
def fetch_merged_data(dates: List[str]) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    placeholders = ",".join(["?"] * len(dates))
    # URL列は汎用名でエクスポート（会社名が読み取れないようにする）
    query = f"""
        SELECT
            c.target_date,
            c.code,
            c.name,
            n.sector,
            n.price,
            n.per,
            n.yield_rate,
            n.pbr,
            n.roe AS roe_n,
            n.earning_yield,
            m.rating,
            m.sales,
            m.profit,
            m.scale,
            m.cheap,
            m.growth,
            m.profitab,
            m.safety,
            m.risk,
            m.return_rate,
            m.liquidity,
            m.trend,
            m.forex,
            m.technical,
            c.nikkeiurl AS ref_url_1,
            c.quickurl  AS ref_url_2,
            c.sbiurl    AS ref_url_3
        FROM consensus_url c
        LEFT JOIN nikkei_reports n
               ON c.target_date = n.target_date AND c.code = n.code
        LEFT JOIN moneyworld_reports m
               ON c.target_date = m.target_date AND c.code = m.code
        WHERE c.target_date IN ({placeholders})
    """
    df = pd.read_sql_query(query, conn, params=dates)
    conn.close()
    return df


# ====== 横持ち化 ======
def reshape_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    縦持ち（target_dateごと）を横持ち化。
    キーは ['code','name','sector'] に統一。
    URLは最後に最新日付のものを code キーで付与。
    """
    if df.empty:
        return df

    # 必須列
    need = {"target_date", "code", "name", "sector"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"必要列が不足しています: {sorted(miss)}")

    # URL は最新日ベースで付与（汎用名）
    url_cols = ["ref_url_1", "ref_url_2", "ref_url_3"]
    url_df = (
        df.sort_values("target_date", ascending=False)
          .drop_duplicates(subset=["code"])
          .set_index("code")[url_cols]
    )

    # 横展開対象（nikkei + moneyworld のみ）
    value_cols = [
        "price", "per", "yield_rate", "pbr", "roe_n", "earning_yield",
        "rating", "sales", "profit", "scale", "cheap", "growth", "profitab",
        "safety", "risk", "return_rate", "liquidity", "trend", "forex", "technical",
    ]

    keys = ["code", "name", "sector"]
    all_dates = sorted(df["target_date"].astype(str).unique(), reverse=True)

    # 日付ごとに "{日本語名}_{YYYYMMDD}" へ改名して作成
    records = []
    for td in all_dates:
        sub = df[df["target_date"] == td].copy()
        # 衝突回避のため余計な列を落とす
        sub = sub.drop(columns=[c for c in ["target_date", *url_cols] if c in sub.columns], errors="ignore")
        # 値カラムに日付サフィックスを付与（存在する列のみ）
        rename_map = {c: f"{COLUMN_NAME_MAP.get(c, c)}_{td}" for c in value_cols if c in sub.columns}
        sub = sub.rename(columns=rename_map)
        # キー＋値列のみを残す
        keep_cols = keys + [c for c in sub.columns if c not in keys]
        sub = sub[keep_cols]
        records.append(sub)

    # 全銘柄のユニークキー集合から順次マージ
    base_keys = df[keys].drop_duplicates().copy()
    merged = base_keys
    for df_ in records:
        dup_cols = [c for c in df_.columns if c in merged.columns and c not in keys]
        if dup_cols:
            df_ = df_.drop(columns=dup_cols, errors="ignore")
        merged = pd.merge(merged, df_, on=keys, how="outer")

    # URL 付与（最新日）
    merged = merged.set_index("code").join(url_df, how="left").reset_index()

    # 列順：コード/企業名/セクター → 各日付の値列 → URL
    ordered = ["code", "name", "sector"]
    for td in all_dates:
        for c in value_cols:
            col = f"{COLUMN_NAME_MAP.get(c, c)}_{td}"
            if col in merged.columns:
                ordered.append(col)
    ordered += [c for c in url_cols if c in merged.columns]
    merged = merged[ordered]

    # 見出し日本語化＋対象日まとめ
    merged = merged.rename(columns={"code": "コード", "name": "企業名", "sector": "セクター"})
    merged.insert(0, "日付", ",".join(all_dates))
    return merged


# ====== シート出力 ======
def sanitize(val):
    return "" if pd.isna(val) else str(val)

def export_to_gsheet(df: pd.DataFrame):
    header = list(df.columns)
    rows = [list(map(sanitize, row)) for _, row in df.iterrows()]
    sh = authorize_gspread()
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=str(max(1000, len(rows)+10)), cols=str(max(50, len(header)+5)))
    ws.clear()
    ws.update([header] + rows)
    print(f"✅ {len(df)}件のデータを Google Sheets に出力しました。 → 『{SPREADSHEET_NAME} / {SHEET_NAME}』")


# ====== Main ======
def main():
    ap = argparse.ArgumentParser(description="DBの3テーブルを統合してGoogle Sheetsへ出力（sbi_reportsは除外）。")
    ap.add_argument("--date", required=True, help="例: 20250825 / 20250801-0831 / 20250801- 0831（年省略可）")
    args = ap.parse_args()

    # 入力日付の解釈
    start_date, end_date = parse_date_range(args.date)

    # 対象日一覧
    dates = get_available_dates_in_range(start_date, end_date)
    if not dates:
        print("⚠️ 指定範囲に利用可能な target_date が見つかりません。")
        return

    # データ取得
    df = fetch_merged_data(dates)
    if df.empty:
        print("⚠️ データが見つかりませんでした。")
        return

    # 横持ち化 → Sheets 出力
    reshaped_df = reshape_data(df)
    export_to_gsheet(reshaped_df)


if __name__ == "__main__":
    main()
