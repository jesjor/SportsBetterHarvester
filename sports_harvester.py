"""
Polymarket Sports Arbitrage Harvester
======================================
Kører to parallelle WebSocket streams:
  1. Polymarket Sports WS  → live score events (mål, periods, etc.)
  2. Polymarket CLOB WS    → realtids prisændringer på alle live sports markeder

Formålet er at måle forsinkelsen (latency window) mellem:
  score event → markedspris reagerer

Alt logges til SQLite med nanosekund timestamps.
"""

import asyncio
import json
import time
import logging
import signal
import os
from datetime import datetime, timezone

import websockets
import aiohttp

from db import Database
from market_discovery import MarketDiscovery

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
SPORTS_WS_URL   = "wss://sports-ws.polymarket.com"
CLOB_WS_URL     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API_URL   = "https://gamma-api.polymarket.com"

SPORTS_PING_INTERVAL = 4        # Polymarket kræver pong inden 10s — vi sender hvert 4. sekund
CLOB_PING_INTERVAL   = 8
MARKET_REFRESH_SECS  = 120      # Opdater listen over live markeder hvert 2. minut

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("harvester")


# ──────────────────────────────────────────────
# Score Event Parser
# ──────────────────────────────────────────────
class ScoreEvent:
    """Repræsenterer en score-ændring opdaget i Sports WS stream."""
    def __init__(self, game_id, league, home, away, old_score, new_score,
                 period, elapsed, ts_ns):
        self.game_id   = game_id
        self.league    = league
        self.home      = home
        self.away      = away
        self.old_score = old_score
        self.new_score = new_score
        self.period    = period
        self.elapsed   = elapsed
        self.ts_ns     = ts_ns          # nanosekund timestamp

    def total_goals(self, score_str):
        """Parse 'home-away' → total mål (football)."""
        try:
            parts = score_str.split("-")
            return sum(int(p) for p in parts)
        except Exception:
            return None

    @property
    def goal_scored(self):
        old = self.total_goals(self.old_score or "")
        new = self.total_goals(self.new_score or "")
        if old is not None and new is not None:
            return new > old
        return False

    @property
    def goal_delta(self):
        old = self.total_goals(self.old_score or "")
        new = self.total_goals(self.new_score or "")
        if old is not None and new is not None:
            return new - old
        return 0

    def __repr__(self):
        return (f"ScoreEvent({self.league} {self.home} vs {self.away} "
                f"{self.old_score}→{self.new_score} @{self.elapsed} {self.period})")


def parse_score_change(old_state: dict, new_msg: dict) -> ScoreEvent | None:
    """Sammenligner to game-states. Returnerer ScoreEvent hvis score ændret sig."""
    if old_state is None:
        return None
    old_score = old_state.get("score", "")
    new_score = new_msg.get("score", "")
    if old_score != new_score and old_score and new_score:
        return ScoreEvent(
            game_id   = new_msg.get("gameId"),
            league    = new_msg.get("leagueAbbreviation", ""),
            home      = new_msg.get("homeTeam", ""),
            away      = new_msg.get("awayTeam", ""),
            old_score = old_score,
            new_score = new_score,
            period    = new_msg.get("period", ""),
            elapsed   = new_msg.get("elapsed", ""),
            ts_ns     = time.time_ns()
        )
    return None


# ──────────────────────────────────────────────
# Sports WebSocket Worker
# ──────────────────────────────────────────────
async def sports_ws_worker(db: Database, score_event_queue: asyncio.Queue):
    """
    Forbinder til Polymarket Sports WS.
    Ingen auth, ingen subscription message nødvendig.
    Sender pong ved server ping.
    Logger alle score-ændringer.
    Hvis Sports WS er utilgængeligt (f.eks. blokeret på Railway), deaktiveres
    denne worker gracefully — CLOB WS fortsætter uforstyrret.
    """
    game_states: dict[int, dict] = {}   # gameId → seneste state
    reconnect_delay = 5.0
    consecutive_failures = 0

    while True:
        try:
            log.info(f"Sports WS: Forbinder til {SPORTS_WS_URL}")
            async with websockets.connect(
                SPORTS_WS_URL,
                ping_interval=None,         # Vi håndterer ping/pong manuelt
                max_size=10 * 1024 * 1024,
            ) as ws:
                reconnect_delay = 5.0
                consecutive_failures = 0
                log.info("Sports WS: Forbundet ✓")

                last_ping = time.monotonic()

                async for raw in ws:
                    recv_ts_ns = time.time_ns()

                    # Polymarket Sports WS sender "ping" som plain string
                    if raw == "ping":
                        await ws.send("pong")
                        last_ping = time.monotonic()
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        log.warning(f"Sports WS: Ugyldigt JSON: {raw[:100]}")
                        continue

                    # Håndter liste af game updates
                    games = msg if isinstance(msg, list) else [msg]

                    for game in games:
                        game_id = game.get("gameId")
                        if not game_id:
                            continue

                        # Check for score-ændring
                        old_state = game_states.get(game_id)
                        score_evt = parse_score_change(old_state, game)

                        # Opdater state
                        game_states[game_id] = game

                        # Log til DB (alle game updates)
                        db.log_sports_update(
                            game_id     = game_id,
                            league      = game.get("leagueAbbreviation", ""),
                            home_team   = game.get("homeTeam", ""),
                            away_team   = game.get("awayTeam", ""),
                            score       = game.get("score", ""),
                            period      = game.get("period", ""),
                            elapsed     = game.get("elapsed", ""),
                            status      = game.get("status", ""),
                            live        = game.get("live", False),
                            ended       = game.get("ended", False),
                            ts_ns       = recv_ts_ns,
                            raw_json    = json.dumps(game)
                        )

                        if score_evt:
                            log.info(f"🥅 SCORE EVENT: {score_evt}")
                            db.log_score_event(score_evt)
                            await score_event_queue.put(score_evt)

                    # Manuel ping hvis server er stille
                    if time.monotonic() - last_ping > SPORTS_PING_INTERVAL:
                        await ws.send("ping")
                        last_ping = time.monotonic()

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"Sports WS: Forbindelse lukket: {e}. Genforbinder om {reconnect_delay}s...")
        except OSError as e:
            consecutive_failures += 1
            if consecutive_failures == 1:
                log.warning(f"Sports WS: Kan ikke nå {SPORTS_WS_URL} ({e}). "
                            f"Sandsynligvis blokeret i dette miljø.")
            if consecutive_failures >= 5:
                log.warning("Sports WS: Permanent utilgængeligt. "
                            "Deaktiverer — CLOB WS kører videre. "
                            "Score events vil ikke blive registreret.")
                return   # Afslut workeren — CLOB WS er upåvirket
        except Exception as e:
            log.error(f"Sports WS fejl: {e}", exc_info=True)

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 120)  # Max 2 min


# ──────────────────────────────────────────────
# CLOB Market WebSocket Worker
# ──────────────────────────────────────────────
async def clob_ws_worker(db: Database, market_ids: list[str],
                          score_event_queue: asyncio.Queue,
                          active_markets_ref: dict):
    """
    Forbinder til Polymarket CLOB Market WS.
    Abonnerer på alle live sports markeder.
    Logger alle prisændringer med timestamp.
    Når en ScoreEvent ankommer, noterer vi latency.
    """
    reconnect_delay = 1.0

    while True:
        # Vent på at active_markets_ref bliver populeret
        current_ids = active_markets_ref.get("asset_ids", [])
        if not current_ids:
            log.info("CLOB WS: Ingen asset IDs endnu — venter 5s...")
            await asyncio.sleep(5)
            continue
        try:
            current_ids = list(active_markets_ref.get("asset_ids", []))
            log.info(f"CLOB WS: Forbinder — {len(current_ids)} markeder")
            async with websockets.connect(
                CLOB_WS_URL,
                ping_interval=None,
                max_size=10 * 1024 * 1024,
            ) as ws:
                reconnect_delay = 1.0

                # Subscribe til alle markeder
                sub_msg = {
                    "type": "market",
                    "assets_ids": current_ids,
                    "custom_feature_enabled": True   # Får market_resolved events
                }
                await ws.send(json.dumps(sub_msg))
                log.info(f"CLOB WS: Abonnerer på {len(current_ids)} asset IDs ✓")

                last_ping = time.monotonic()

                async for raw in ws:
                    recv_ts_ns = time.time_ns()

                    if time.monotonic() - last_ping > CLOB_PING_INTERVAL:
                        await ws.send(json.dumps({"type": "PING"}))
                        last_ping = time.monotonic()

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    msgs = msg if isinstance(msg, list) else [msg]

                    for m in msgs:
                        event_type = m.get("event_type", "")
                        asset_id   = m.get("asset_id", "")
                        market_id  = m.get("market", "")

                        if event_type == "price_change":
                            price      = float(m.get("price", 0))
                            size       = float(m.get("size", 0))
                            side       = m.get("side", "")
                            best_bid   = m.get("best_bid")
                            best_ask   = m.get("best_ask")

                            db.log_price_event(
                                asset_id   = asset_id,
                                market_id  = market_id,
                                price      = price,
                                size       = size,
                                side       = side,
                                best_bid   = float(best_bid) if best_bid else None,
                                best_ask   = float(best_ask) if best_ask else None,
                                ts_ns      = recv_ts_ns,
                                raw_json   = json.dumps(m)
                            )

                        elif event_type == "market_resolved":
                            log.info(f"✅ MARKET RESOLVED: {market_id} → {m.get('outcome')}")
                            db.log_resolution(
                                market_id  = market_id,
                                asset_id   = asset_id,
                                outcome    = m.get("outcome", ""),
                                ts_ns      = recv_ts_ns,
                                raw_json   = json.dumps(m)
                            )

                        elif event_type == "tick_size_change":
                            db.log_tick_size_change(
                                asset_id     = asset_id,
                                market_id    = market_id,
                                old_tick     = m.get("old_tick_size", ""),
                                new_tick     = m.get("new_tick_size", ""),
                                ts_ns        = recv_ts_ns
                            )

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"CLOB WS: Lukket: {e}. Genforbinder om {reconnect_delay}s...")
        except Exception as e:
            log.error(f"CLOB WS fejl: {e}", exc_info=True)

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 30)


# ──────────────────────────────────────────────
# Score → Price Latency Analyzer
# ──────────────────────────────────────────────
async def latency_analyzer(db: Database, score_event_queue: asyncio.Queue):
    """
    Lytter på ScoreEvents fra køen.
    For hvert event: find næste prisændring i DB for relaterede markeder.
    Beregner latency window og logger det.
    """
    while True:
        try:
            evt: ScoreEvent = await asyncio.wait_for(
                score_event_queue.get(), timeout=5.0
            )
            log.info(f"⏱  Analyzer: behandler {evt}")

            # Vent 0.5s for at give CLOB WS tid til at modtage og logge prisændringen
            await asyncio.sleep(0.5)

            # Find prisændringer der skete EFTER score eventet
            latencies = db.get_latencies_after_event(evt.game_id, evt.ts_ns)
            if latencies:
                for lat in latencies:
                    log.info(
                        f"  📊 Latency: {lat['asset_id'][:12]}... "
                        f"price {lat['old_price']:.3f}→{lat['new_price']:.3f} "
                        f"delay={lat['delay_ms']:.0f}ms"
                    )
            else:
                log.info(f"  ⚠️  Ingen prisreaktion fundet inden 30s for game {evt.game_id}")

        except asyncio.TimeoutError:
            pass
        except Exception as e:
            log.error(f"Latency analyzer fejl: {e}", exc_info=True)


# ──────────────────────────────────────────────
# Market Discovery Loop
# ──────────────────────────────────────────────
async def market_refresh_loop(discovery: MarketDiscovery, active_markets: dict,
                               clob_restart_event: asyncio.Event):
    """
    Opdaterer listen over live sports markeder periodisk.
    Trigger CLOB WS genforbind hvis nye markeder opdages.
    """
    while True:
        try:
            log.info("Market discovery: Henter live sports markeder...")
            markets = await discovery.fetch_live_sports_markets()

            new_asset_ids = set()
            for m in markets:
                # Prøv clobTokenIds først (direkte array af strings) — primær kilde
                clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or []
                if clob_ids:
                    for tid in clob_ids:
                        if tid:
                            new_asset_ids.add(str(tid))
                else:
                    # Fallback: tokens array
                    for token in m.get("tokens", []):
                        tid = token.get("token_id") or token.get("tokenId")
                        if tid:
                            new_asset_ids.add(str(tid))

            old_ids = set(active_markets.get("asset_ids", []))
            if new_asset_ids != old_ids:
                added   = new_asset_ids - old_ids
                removed = old_ids - new_asset_ids
                if added:
                    log.info(f"Market discovery: {len(added)} nye assets opdaget")
                if removed:
                    log.info(f"Market discovery: {len(removed)} assets udgået")
                active_markets["asset_ids"] = list(new_asset_ids)
                active_markets["markets"]   = markets
                clob_restart_event.set()

            log.info(f"Market discovery: {len(markets)} markeder, "
                     f"{len(new_asset_ids)} asset IDs aktive")

        except Exception as e:
            log.error(f"Market discovery fejl: {e}", exc_info=True)

        await asyncio.sleep(MARKET_REFRESH_SECS)


# ──────────────────────────────────────────────
# Stats Printer
# ──────────────────────────────────────────────
async def stats_printer(db: Database):
    """Printer løbende statistik hvert minut."""
    while True:
        await asyncio.sleep(60)
        try:
            stats = db.get_stats()
            log.info(
                f"📈 STATS | "
                f"Score events: {stats['score_events']} | "
                f"Price events: {stats['price_events']} | "
                f"Resolutions: {stats['resolutions']} | "
                f"DB size: {stats['db_size_mb']:.1f} MB"
            )
            # Top latency vinduerne
            top = db.get_top_latencies(5)
            if top:
                log.info("Top latencies:")
                for row in top:
                    log.info(f"  {row['league']} | {row['market_type']} | "
                             f"avg={row['avg_ms']:.0f}ms | n={row['count']}")
        except Exception as e:
            log.error(f"Stats printer fejl: {e}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
async def main():
    log.info("=" * 60)
    log.info("  Polymarket Sports Arbitrage Harvester")
    log.info("=" * 60)

    db        = Database("harvester.db")
    discovery = MarketDiscovery(GAMMA_API_URL)

    score_event_queue = asyncio.Queue()
    active_markets    = {"asset_ids": [], "markets": []}
    clob_restart_evt  = asyncio.Event()

    # Initial market fetch
    log.info("Henter initial liste over live markeder...")
    try:
        markets = await discovery.fetch_live_sports_markets()
        for m in markets:
            # Prøv clobTokenIds først (direkte array af strings) — primær kilde
            clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or []
            if clob_ids:
                for tid in clob_ids:
                    if tid:
                        active_markets["asset_ids"].append(str(tid))
            else:
                for token in m.get("tokens", []):
                    tid = token.get("token_id") or token.get("tokenId")
                    if tid:
                        active_markets["asset_ids"].append(str(tid))
        active_markets["markets"] = markets
        log.info(f"Fundet {len(markets)} live sports markeder, "
                 f"{len(active_markets['asset_ids'])} asset IDs")
    except Exception as e:
        log.warning(f"Initial market fetch fejlede: {e}. Starter med tom liste.")

    # Kør alle workers parallelt
    tasks = [
        asyncio.create_task(
            sports_ws_worker(db, score_event_queue),
            name="sports-ws"
        ),
        asyncio.create_task(
            clob_ws_worker(db, active_markets["asset_ids"],
                           score_event_queue, active_markets),
            name="clob-ws"
        ),
        asyncio.create_task(
            latency_analyzer(db, score_event_queue),
            name="latency-analyzer"
        ),
        asyncio.create_task(
            market_refresh_loop(discovery, active_markets, clob_restart_evt),
            name="market-refresh"
        ),
        asyncio.create_task(
            stats_printer(db),
            name="stats-printer"
        ),
    ]

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])

    log.info("Harvester kører. Ctrl+C for at stoppe.")
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("Shutting down...")
    finally:
        db.close()
        log.info("Database lukket. Farvel.")


if __name__ == "__main__":
    asyncio.run(main())
