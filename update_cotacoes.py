import os
import datetime as dt
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].strip().rstrip("/")
ANON = os.environ["SUPABASE_ANON_KEY"]

headers = {
    "apikey": ANON,
    "Authorization": f"Bearer {ANON}",
    "Content-Type": "application/json",
}

def get_tickers():
    url = f"{SUPABASE_URL}/rest/v1/tickers_para_atualizar?select=ticker&ativo=eq.true"
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return [row["ticker"] for row in r.json()]

def fetch_price_yahoo(ticker):
    symbol = f"{ticker}.SA"
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()["quoteResponse"]["result"]
    if not data:
        raise ValueError("Ticker não encontrado")
    price = data[0].get("regularMarketPrice")
    if price is None:
        raise ValueError("Sem preço")
    return float(price)

def upsert(ticker, price):
    today = dt.date.today().isoformat()
    url = f"{SUPABASE_URL}/rest/v1/cotacoes"
    payload = [{
        "ticker": ticker,
        "data": today,
        "preco": price,
        "fonte": "yahoo"
    }]
    r = requests.post(
        url,
        headers={**headers, "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30
    )
    r.raise_for_status()

def main():
    tickers = get_tickers()
    for t in tickers:
        try:
            price = fetch_price_yahoo(t)
            upsert(t, price)
            print("OK", t, price)
        except Exception as e:
            print("ERRO", t, e)

if __name__ == "__main__":
    main()
