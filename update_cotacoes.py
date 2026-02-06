import os
import datetime as dt
import requests
import time

SUPABASE_URL = os.environ["SUPABASE_URL"].strip().rstrip("/")
ANON = os.environ["SUPABASE_ANON_KEY"].strip()
BRAPI_TOKEN = os.environ["BRAPI_TOKEN"].strip()

supabase_headers = {
    "apikey": ANON,
    "Authorization": f"Bearer {ANON}",
    "Content-Type": "application/json",
}

def get_tickers():
    url = f"{SUPABASE_URL}/rest/v1/tickers_para_atualizar?select=ticker&ativo=eq.true"
    r = requests.get(url, headers=supabase_headers, timeout=30)
    r.raise_for_status()
    return [row["ticker"] for row in r.json()]

def fetch_price_brapi(ticker: str) -> float:
    url = f"https://brapi.dev/api/quote/{ticker}"
    params = {"token": BRAPI_TOKEN}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    results = j.get("results", [])
    if not results:
        raise ValueError(f"Sem results na brapi para {ticker}: {j}")
    price = results[0].get("regularMarketPrice")
    if price is None:
        raise ValueError(f"Sem regularMarketPrice na brapi para {ticker}: {results[0]}")
    return float(price)

def upsert_cotacao(ticker: str, data: str, preco: float, fonte: str = "brapi"):
    url = f"{SUPABASE_URL}/rest/v1/cotacoes"
    payload = [{"ticker": ticker, "data": data, "preco": preco, "fonte": fonte}]
    r = requests.post(
        url,
        headers={**supabase_headers, "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()

def main():
    today = dt.date.today().isoformat()
    tickers = get_tickers()

    if not tickers:
        print("Nenhum ticker ativo para atualizar.")
        return

    ok = 0
    for t in tickers:
        try:
            price = fetch_price_brapi(t)
            upsert_cotacao(t, today, price, "brapi")
            ok += 1
            print(f"OK {t} {price}")
            # pequeno intervalo para respeitar limites do provedor
            time.sleep(0.6)
        except Exception as e:
            print(f"ERRO {t} {e}")

    print(f"Finalizado. OK={ok}/{len(tickers)}")

if __name__ == "__main__":
    main()
