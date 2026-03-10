"""
Database lag til Sports Arbitrage Harvester
SQLite med WAL mode for hurtig concurrent write/read.
Alle timestamps er nanosekunder (time.time_ns()).
"""

import sqlite3
import time
import json
import os
import logging
from contextlib import contextmanager

log = logging.getLogger("db")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA cache_size=-32000;

-- Live score updates fra Sports WS
CREATE TABLE IF NOT EXISTS sports_updates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id     INTEGER NOT NULL,
    league      TEXT,
    home_team   TEXT,
    away_team   TEXT,
    score       TEXT,
    period      TEXT,
    elapsed     TEXT,
    status      TEXT,
    live        INTEGER,
    ended       INTEGER,
    ts_ns       INTEGER NOT NULL,
    raw_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_sports_updates_game_ts ON sports_updates(game_id, ts_ns);
CREATE INDEX IF NOT EXISTS idx_sports_updates_ts ON sports_updates(ts_ns);

-- Score events (kun når score faktisk ændrer sig)
CREATE TABLE IF NOT EXISTS score_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id     INTEGER NOT NULL,
    league      TEXT,
    home_team   TEXT,
    away_team   TEXT,
    old_score   TEXT,
    new_score   TEXT,
    period      TEXT,
    elapsed     TEXT,
    goal_delta  INTEGER,
    ts_ns       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_score_events_game ON score_events(game_id, ts_ns);

-- Prisændringer fra CLOB WS
CREATE TABLE IF NOT EXISTS price_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id    TEXT NOT NULL,
    market_id   TEXT,
    price       REAL,
    size        REAL,
    side        TEXT,
    best_bid    REAL,
    best_ask    REAL,
    ts_ns       INTEGER NOT NULL,
    raw_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_price_events_asset_ts ON price_events(asset_id, ts_ns);
CREATE INDEX IF NOT EXISTS idx_price_events_ts ON price_events(ts_ns);

-- Market resolutions
CREATE TABLE IF NOT EXISTS resolutions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id   TEXT NOT NULL,
    asset_id    TEXT,
    outcome     TEXT,
    ts_ns       INTEGER NOT NULL,
    raw_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_resolutions_market ON resolutions(market_id);

-- Tick size changes (indikerer markedet nærmer sig resolution)
CREATE TABLE IF NOT EXISTS tick_size_changes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id    TEXT,
    market_id   TEXT,
    old_tick    TEXT,
    new_tick    TEXT,
    ts_ns       INTEGER NOT NULL
);

-- Beregnede latency vinduer (score event → pris reagerer)
CREATE TABLE IF NOT EXISTS latency_windows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    score_event_id  INTEGER REFERENCES score_events(id),
    game_id         INTEGER,
    league          TEXT,
    asset_id        TEXT,
    market_id       TEXT,
    score_ts_ns     INTEGER,    -- Tidspunkt for score event
    price_ts_ns     INTEGER,    -- Tidspunkt for første prisreaktion
    delay_ms        REAL,       -- Forsinkelse i millisekunder
    old_price       REAL,
    new_price       REAL,
    price_delta     REAL,
    market_type     TEXT        -- f.eks. "over_1.5", "both_score", "winner"
);
CREATE INDEX IF NOT EXISTS idx_latency_game ON latency_windows(game_id);
CREATE INDEX IF NOT EXISTS idx_latency_league ON latency_windows(league);

-- Market metadata cache (fra Gamma API)
CREATE TABLE IF NOT EXISTS markets_meta (
    condition_id    TEXT PRIMARY KEY,
    slug            TEXT,
    question        TEXT,
    sport_tag       TEXT,
    league_tag      TEXT,
    home_team       TEXT,
    away_team       TEXT,
    game_start_time TEXT,
    token_ids       TEXT,       -- JSON array
    updated_at      INTEGER     -- ts_ns
);
"""


class Database:
    def __init__(self, db_path: str = "harvester.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        log.info(f"Database initialiseret: {db_path}")

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def log_sports_update(self, game_id, league, home_team, away_team,
                          score, period, elapsed, status, live, ended,
                          ts_ns, raw_json):
        self.conn.execute("""
            INSERT INTO sports_updates
            (game_id, league, home_team, away_team, score, period,
             elapsed, status, live, ended, ts_ns, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (game_id, league, home_team, away_team, score, period,
              elapsed, status, int(live), int(ended), ts_ns, raw_json))
        self.conn.commit()

    def log_score_event(self, evt):
        self.conn.execute("""
            INSERT INTO score_events
            (game_id, league, home_team, away_team, old_score, new_score,
             period, elapsed, goal_delta, ts_ns)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (evt.game_id, evt.league, evt.home, evt.away,
              evt.old_score, evt.new_score, evt.period,
              evt.elapsed, evt.goal_delta, evt.ts_ns))
        self.conn.commit()

    def log_price_event(self, asset_id, market_id, price, size, side,
                        best_bid, best_ask, ts_ns, raw_json):
        self.conn.execute("""
            INSERT INTO price_events
            (asset_id, market_id, price, size, side, best_bid, best_ask,
             ts_ns, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (asset_id, market_id, price, size, side,
              best_bid, best_ask, ts_ns, raw_json))
        self.conn.commit()

    def log_resolution(self, market_id, asset_id, outcome, ts_ns, raw_json):
        self.conn.execute("""
            INSERT INTO resolutions (market_id, asset_id, outcome, ts_ns, raw_json)
            VALUES (?,?,?,?,?)
        """, (market_id, asset_id, outcome, ts_ns, raw_json))
        self.conn.commit()

    def log_tick_size_change(self, asset_id, market_id, old_tick, new_tick, ts_ns):
        self.conn.execute("""
            INSERT INTO tick_size_changes
            (asset_id, market_id, old_tick, new_tick, ts_ns)
            VALUES (?,?,?,?,?)
        """, (asset_id, market_id, old_tick, new_tick, ts_ns))
        self.conn.commit()

    def get_latencies_after_event(self, game_id: int, score_ts_ns: int,
                                   window_ns: int = 30_000_000_000) -> list[dict]:
        """
        Find alle prisændringer der skete inden for window_ns nanosekunder
        efter et score event for et givent game.
        Joiner via markets_meta for at finde relaterede asset_ids.
        """
        # Find asset IDs relateret til dette game via markets_meta
        rows = self.conn.execute("""
            SELECT token_ids FROM markets_meta
            WHERE home_team || away_team LIKE '%' || ? || '%'
            OR home_team || away_team LIKE '%' || ? || '%'
        """, (str(game_id), str(game_id))).fetchall()

        asset_ids = []
        for row in rows:
            try:
                ids = json.loads(row["token_ids"] or "[]")
                asset_ids.extend(ids)
            except Exception:
                pass

        if not asset_ids:
            # Fallback: tag alle prisændringer i vinduet (bredt)
            cutoff_ns = score_ts_ns + window_ns
            price_rows = self.conn.execute("""
                SELECT asset_id, price, best_bid, best_ask, ts_ns
                FROM price_events
                WHERE ts_ns BETWEEN ? AND ?
                ORDER BY ts_ns ASC
                LIMIT 50
            """, (score_ts_ns, cutoff_ns)).fetchall()
        else:
            placeholders = ",".join("?" * len(asset_ids))
            cutoff_ns = score_ts_ns + window_ns
            price_rows = self.conn.execute(f"""
                SELECT asset_id, price, best_bid, best_ask, ts_ns
                FROM price_events
                WHERE asset_id IN ({placeholders})
                AND ts_ns BETWEEN ? AND ?
                ORDER BY ts_ns ASC
            """, (*asset_ids, score_ts_ns, cutoff_ns)).fetchall()

        results = []
        seen_assets = set()
        for row in price_rows:
            if row["asset_id"] not in seen_assets:
                seen_assets.add(row["asset_id"])
                # Hent pris FØR score event
                prev = self.conn.execute("""
                    SELECT price FROM price_events
                    WHERE asset_id = ? AND ts_ns < ?
                    ORDER BY ts_ns DESC LIMIT 1
                """, (row["asset_id"], score_ts_ns)).fetchone()
                results.append({
                    "asset_id":  row["asset_id"],
                    "old_price": prev["price"] if prev else None,
                    "new_price": row["price"],
                    "delay_ms":  (row["ts_ns"] - score_ts_ns) / 1_000_000,
                })

        return results

    def get_stats(self) -> dict:
        score_events = self.conn.execute(
            "SELECT COUNT(*) as c FROM score_events"
        ).fetchone()["c"]
        price_events = self.conn.execute(
            "SELECT COUNT(*) as c FROM price_events"
        ).fetchone()["c"]
        resolutions = self.conn.execute(
            "SELECT COUNT(*) as c FROM resolutions"
        ).fetchone()["c"]
        db_size = os.path.getsize(self.db_path) / (1024 * 1024)
        return {
            "score_events": score_events,
            "price_events": price_events,
            "resolutions":  resolutions,
            "db_size_mb":   db_size,
        }

    def get_top_latencies(self, limit: int = 10) -> list[dict]:
        rows = self.conn.execute("""
            SELECT league, market_type,
                   AVG(delay_ms) as avg_ms,
                   MIN(delay_ms) as min_ms,
                   MAX(delay_ms) as max_ms,
                   COUNT(*) as count
            FROM latency_windows
            GROUP BY league, market_type
            ORDER BY avg_ms ASC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def save_market_meta(self, condition_id, slug, question, sport_tag,
                          league_tag, home_team, away_team, game_start_time,
                          token_ids):
        self.conn.execute("""
            INSERT OR REPLACE INTO markets_meta
            (condition_id, slug, question, sport_tag, league_tag,
             home_team, away_team, game_start_time, token_ids, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (condition_id, slug, question, sport_tag, league_tag,
              home_team, away_team, game_start_time,
              json.dumps(token_ids), time.time_ns()))
        self.conn.commit()

    def close(self):
        self.conn.close()
