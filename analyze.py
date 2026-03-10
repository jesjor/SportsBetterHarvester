"""
Arbitrage Latency Analyzer
============================
Kør dette script mens harvesteren kører (eller bagefter) for at
analysere indsamlede data og identificere arbitrage vinduer.

Brug:
  python analyze.py                    # Live analyse
  python analyze.py --db harvester.db  # Specifik database
  python analyze.py --report           # Generer fuld rapport
"""

import sqlite3
import argparse
import json
from datetime import datetime, timezone


def ts_ns_to_str(ts_ns: int) -> str:
    """Konverter nanosekund timestamp til læsbar streng."""
    return datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).strftime("%H:%M:%S.%f")


def analyze_latency_windows(conn: sqlite3.Connection):
    """
    Beregner latency vinduer:
    For hvert score event → find første prisreaktion på relaterede markeder.
    """
    print("\n" + "="*70)
    print("  LATENCY WINDOWS ANALYSE")
    print("="*70)

    # Hent alle score events
    score_events = conn.execute("""
        SELECT id, game_id, league, home_team, away_team,
               old_score, new_score, period, elapsed, ts_ns
        FROM score_events
        ORDER BY ts_ns DESC
        LIMIT 100
    """).fetchall()

    if not score_events:
        print("Ingen score events endnu. Harvester skal køre under live kampe.")
        return

    print(f"\nAnalyserer {len(score_events)} score events...\n")

    all_latencies = []

    for evt in score_events:
        game_id    = evt["game_id"]
        score_ts   = evt["ts_ns"]
        window_ns  = 60_000_000_000  # 60 sekunder vindue

        # Find prisændringer efter score event
        price_rows = conn.execute("""
            SELECT asset_id, market_id, price, best_bid, best_ask, ts_ns
            FROM price_events
            WHERE ts_ns BETWEEN ? AND ?
            ORDER BY ts_ns ASC
        """, (score_ts, score_ts + window_ns)).fetchall()

        if not price_rows:
            continue

        # Første prisreaktion
        first = price_rows[0]
        delay_ms = (first["ts_ns"] - score_ts) / 1_000_000

        # Hent pris FØR event
        prev_price = conn.execute("""
            SELECT price FROM price_events
            WHERE asset_id = ? AND ts_ns < ?
            ORDER BY ts_ns DESC LIMIT 1
        """, (first["asset_id"], score_ts)).fetchone()

        price_delta = None
        if prev_price:
            price_delta = first["price"] - prev_price["price"]

        print(f"⚽ {evt['league'].upper():6} | "
              f"{evt['home_team']} vs {evt['away_team']} | "
              f"{evt['old_score']} → {evt['new_score']} | "
              f"{evt['period']} {evt['elapsed']}")
        print(f"   Tid:      {ts_ns_to_str(score_ts)}")
        print(f"   Reaktion: {delay_ms:.0f}ms | "
              f"Pris: {prev_price['price'] if prev_price else '?':.3f} → {first['price']:.3f} "
              f"({'↑' if price_delta and price_delta > 0 else '↓'}{abs(price_delta):.3f} "
              f"if price_delta else '')")
        print()

        all_latencies.append(delay_ms)

    if all_latencies:
        all_latencies.sort()
        n = len(all_latencies)
        print(f"\nLatency Statistik ({n} observationer):")
        print(f"  Minimum:  {min(all_latencies):.0f}ms")
        print(f"  Median:   {sorted(all_latencies)[n//2]:.0f}ms")
        print(f"  P90:      {sorted(all_latencies)[int(n*0.9)]:.0f}ms")
        print(f"  Maximum:  {max(all_latencies):.0f}ms")
        avg = sum(all_latencies) / n
        print(f"  Gennemsnit: {avg:.0f}ms")


def analyze_market_types(conn: sqlite3.Connection):
    """Hvilke market typer reagerer hurtigst/langsomst?"""
    print("\n" + "="*70)
    print("  MARKET TYPE REAKTIONER")
    print("="*70)

    rows = conn.execute("""
        SELECT market_type, COUNT(*) as n,
               AVG(delay_ms) as avg_ms,
               MIN(delay_ms) as min_ms,
               MAX(delay_ms) as max_ms
        FROM latency_windows
        GROUP BY market_type
        ORDER BY avg_ms ASC
    """).fetchall()

    if not rows:
        print("Ingen latency windows beregnet endnu.")
        return

    print(f"\n{'Type':<25} {'N':>5} {'Min':>8} {'Avg':>8} {'Max':>8}")
    print("-" * 58)
    for row in rows:
        print(f"{row['market_type']:<25} {row['n']:>5} "
              f"{row['min_ms']:>7.0f}ms {row['avg_ms']:>7.0f}ms {row['max_ms']:>7.0f}ms")


def show_recent_activity(conn: sqlite3.Connection):
    """Viser recent aktivitet."""
    print("\n" + "="*70)
    print("  RECENT AKTIVITET")
    print("="*70)

    # Seneste score events
    print("\n📊 Seneste score events:")
    rows = conn.execute("""
        SELECT league, home_team, away_team, old_score, new_score,
               period, elapsed, ts_ns
        FROM score_events
        ORDER BY ts_ns DESC
        LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {ts_ns_to_str(r['ts_ns'])} | "
              f"{r['league']:6} | {r['home_team']} vs {r['away_team']} | "
              f"{r['old_score']}→{r['new_score']} | {r['period']} {r['elapsed']}")

    # Seneste resolutions
    print("\n✅ Seneste market resolutions:")
    rows = conn.execute("""
        SELECT market_id, outcome, ts_ns
        FROM resolutions
        ORDER BY ts_ns DESC
        LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {ts_ns_to_str(r['ts_ns'])} | "
              f"{r['market_id'][:20]}... | Outcome: {r['outcome']}")

    # Database stats
    print("\n📈 Database statistik:")
    for table in ["sports_updates", "score_events", "price_events", "resolutions"]:
        count = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()["c"]
        print(f"  {table:<25}: {count:,} rækker")


def show_live_games(conn: sqlite3.Connection):
    """Vis igangværende kampe."""
    print("\n" + "="*70)
    print("  IGANGVÆRENDE KAMPE")
    print("="*70)

    rows = conn.execute("""
        SELECT DISTINCT game_id, league, home_team, away_team, score,
                        period, elapsed, ts_ns
        FROM sports_updates
        WHERE live = 1 AND ended = 0
        AND ts_ns > (SELECT MAX(ts_ns) - 300000000000 FROM sports_updates)
        ORDER BY ts_ns DESC
    """).fetchall()

    seen = set()
    for r in rows:
        key = r["game_id"]
        if key in seen:
            continue
        seen.add(key)
        print(f"  {r['league']:6} | {r['home_team']} vs {r['away_team']} | "
              f"Score: {r['score']} | {r['period']} {r['elapsed']} | "
              f"Sidst set: {ts_ns_to_str(r['ts_ns'])}")


def main():
    parser = argparse.ArgumentParser(description="Polymarket Sports Arbitrage Analyzer")
    parser.add_argument("--db", default="harvester.db", help="Sti til SQLite database")
    parser.add_argument("--report", action="store_true", help="Generer fuld rapport")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    print(f"\nPolymarket Sports Arbitrage Analyzer")
    print(f"Database: {args.db}")

    show_live_games(conn)
    show_recent_activity(conn)
    analyze_latency_windows(conn)

    if args.report:
        analyze_market_types(conn)

    conn.close()


if __name__ == "__main__":
    main()
