# Polymarket Sports Arbitrage Harvester

## Arkitektur

```
┌─────────────────────────────────────────────────────────────┐
│                     Railway (24/7)                          │
│                                                             │
│  ┌──────────────────┐     ┌──────────────────┐             │
│  │   Sports WS      │     │    CLOB WS        │             │
│  │ (score events)   │     │  (prisændringer)  │             │
│  │                  │     │                   │             │
│  │ wss://sports-ws  │     │ wss://ws-sub-     │             │
│  │ .polymarket.com  │     │ clob.polymarket.. │             │
│  └────────┬─────────┘     └────────┬──────────┘             │
│           │                        │                        │
│           └──────────┬─────────────┘                        │
│                      ▼                                       │
│              ┌───────────────┐                              │
│              │  SQLite DB    │                              │
│              │               │                              │
│              │ sports_updates│                              │
│              │ score_events  │ ← Mål! 1-0 → 1-1            │
│              │ price_events  │ ← Pris 0.3 → 0.97           │
│              │ resolutions   │                              │
│              │ latency_win.. │ ← delay: 4200ms             │
│              └───────────────┘                              │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Market Discovery (hvert 2. min via Gamma API)        │  │
│  │  → Holder listen over abonnerede assets opdateret     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Hvad vi måler

**Latency vinduet** = tid fra score event ankommer i Sports WS
til prisændring registreres i CLOB WS.

Dette er det vindue vi kan udnytte til arbitrage.

## Filer

| Fil | Formål |
|-----|--------|
| `sports_harvester.py` | Main - kører alle workers med asyncio |
| `db.py` | SQLite database lag |
| `market_discovery.py` | Henter live sports markeder fra Gamma API |
| `analyze.py` | Analyse og rapport script |

## Deploy på Railway

```bash
# Tilføj til eksisterende Railway projekt
railway up

# Eller som ny service
railway new
railway up
```

## Kør lokalt

```bash
pip install -r requirements.txt
python sports_harvester.py

# I andet terminal - løbende analyse
python analyze.py
```

## Environment Variables

| Variabel | Standard | Beskrivelse |
|----------|----------|-------------|
| `LOG_LEVEL` | `INFO` | `DEBUG` for verbose |

## Næste skridt (efter data indsamling)

Når vi har 2-4 ugers data:

1. **Kør `python analyze.py --report`** — se hvilke market typer der er
   langsomst til at reagere (= størst arbitrage vindue)

2. **VAR analyse** — se hvor mange "mål events" der efterfølges af
   annullering inden for 90 sekunder

3. **Likviditets analyse** — hvilke markeder har nok volumen til at
   vi faktisk kan fylde en ordre

4. **Bot v1** — byg trading bot der handler på de bedste markeder
   baseret på data
