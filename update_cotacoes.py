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

def get_symbol_map():
    url = f"{SUPABASE_URL}/rest/v1/ticker_map?select=ticker,brapi_symbol"
    r = requests.get(url, headers=supabase_headers, timeout=30)
    r.raise_for_status()
    return {row["ticker"]: row["brapi_symbol"] for row in r.json()}

def get_tickers():
    url = f"{SUPABASE_URL}/rest/v1/tickers_para_atualizar?select=ticker&ativo=eq.true"
    r = requests.get(url, headers=supabase_headers, timeout=30)
    r.raise_for_status()
    return [row["ticker"] for row in r.json()]

def fetch_price_brapi(symbol: str) -> float:
    url = f"https://brapi.dev/api/quote/{symbol}"
    params = {"token": BRAPI_TOKEN}
    r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()

    j = r.json()
    results = j.get("results", [])
    if not results:
        raise ValueError(f"Sem results para {symbol}: {j}")

    price = results[0].get("regularMarketPrice")
    if price is None:
        raise ValueError(f"Sem regularMarketPrice para {symbol}: {results[0]}")

    price = float(price)
    if price <= 0:
        raise ValueError(f"Preço inválido ({price}) para {symbol}")

    return price

def upsert_cotacao(ticker: str, data: str, preco: float, fonte: str = "brapi"):
    url = f"{SUPABASE_URL}/rest/v1/cotacoes"
    payload = [{"ticker": ticker, "data": data, "preco": preco, "fonte": fonte}]
    r = requests.post(
        url,
        headers={**supabase_headers, "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    if not r.ok:
        raise ValueError(f"Supabase {r.status_code}: {r.text}")


def main():
    today = dt.date.today().isoformat()

    tickers = get_tickers()
    if not tickers:
        print("Nenhum ticker ativo para atualizar.")
        return

    symbol_map = get_symbol_map()

    ok = 0
    for t in tickers:
        # Ordem de tentativa:
        # 1) ticker normal (t)
        # 2) ticker_map (se existir e for diferente)
        # 3) fallback com "F" (ex: BBAS3F)
        candidates = [t]

        mapped = symbol_map.get(t)
        if mapped and mapped != t:
            candidates.append(mapped)

        if not t.endswith("F"):
            candidates.append(f"{t}F")

        price = None
        used = None
        last_err = None

        for sym in candidates:
            try:
                price = fetch_price_brapi(sym)  # agora rejeita 0 automaticamente
                used = sym
                break
            except Exception as e:
                last_err = e

        if price is None:
            print(f"ERRO {t} tentativas={candidates} => {last_err}")
            continue

        try:
            upsert_cotacao(t, today, price, "brapi")
            ok += 1
            print(f"OK {t} ({used}) {price}")
            time.sleep(0.6)
        except Exception as e:
            print(f"ERRO-SUPABASE {t} ({used}) {e}")

    print(f"Finalizado. OK={ok}/{len(tickers)}")

if __name__ == "__main__":
    main()
