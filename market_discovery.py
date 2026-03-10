"""
Market Discovery
================
Henter alle aktive live sports markeder fra Polymarket's Gamma API.
Parser token IDs og metadata til brug i CLOB WS subscription.

FIXES:
- Bug fix: Gamma API bruger 'clobTokenIds' (array af strings), ikke 'tokens[].token_id'
- Bug fix: Fallback /markets endpoint har samme token-parsing fix
- Fjernet afhængighed af sports-ws.polymarket.com (blokeret på Railway)
"""

import asyncio
import logging
from typing import Optional

import aiohttp

log = logging.getLogger("market_discovery")


def _extract_token_ids(market: dict) -> list[str]:
    """
    Udtrækker token IDs fra et Gamma market objekt.
    Prøver alle kendte felt-navne i prioriteret rækkefølge.
    """
    # Primær: clobTokenIds er et simpelt array af ID-strings
    clob_ids = market.get("clobTokenIds") or market.get("clob_token_ids") or []
    if clob_ids:
        return [str(tid) for tid in clob_ids if tid]

    # Sekundær: tokens array med token_id/tokenId felt
    token_ids = []
    for token in market.get("tokens", []):
        tid = token.get("token_id") or token.get("tokenId")
        if tid:
            token_ids.append(str(tid))
    if token_ids:
        return token_ids

    # Tertiær: allerede parset _token_ids
    return [str(t) for t in market.get("_token_ids", []) if t]


class MarketDiscovery:
    def __init__(self, gamma_api_url: str = "https://gamma-api.polymarket.com"):
        self.base_url = gamma_api_url

    async def fetch_live_sports_markets(self) -> list[dict]:
        """
        Henter alle markets der:
        - Er aktive (active=true)
        - Er sports-relaterede (tag: sports)
        - Har gyldige token IDs
        """
        all_markets = []

        async with aiohttp.ClientSession() as session:
            markets = await self._fetch_events_sports(session)
            all_markets.extend(markets)

            # Dedup på condition_id
            seen = set()
            deduped = []
            for m in all_markets:
                cid = m.get("conditionId") or m.get("condition_id") or m.get("id")
                if cid and cid not in seen:
                    seen.add(cid)
                    deduped.append(m)

        log.info(f"Market discovery: {len(deduped)} unikke markeder fundet")
        return deduped

    async def _fetch_events_sports(self, session: aiohttp.ClientSession) -> list[dict]:
        """Henter sports events via Gamma Events API."""
        markets = []

        try:
            url = f"{self.base_url}/events"
            params = {
                "active":   "true",
                "closed":   "false",
                "limit":    500,
                "tag_slug": "sports",
            }

            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    log.warning(f"Gamma events API: status {resp.status}")
                    return []
                data = await resp.json()

            events = data if isinstance(data, list) else data.get("events", [])
            log.info(f"Gamma API: {len(events)} sports events")

            for event in events:
                for market in event.get("markets", []):
                    if not market.get("active", False):
                        continue
                    if market.get("closed", False):
                        continue

                    token_ids = _extract_token_ids(market)
                    if token_ids:
                        market["_token_ids"]    = token_ids
                        market["_event_title"]  = event.get("title", "")
                        market["_event_slug"]   = event.get("slug", "")
                        markets.append(market)

            if markets:
                log.info(f"Events endpoint: {len(markets)} aktive markets med token IDs")

        except asyncio.TimeoutError:
            log.warning("Gamma API timeout på events endpoint")
        except Exception as e:
            log.error(f"Gamma API fejl: {e}", exc_info=True)

        # Fallback: prøv direkte /markets endpoint
        if not markets:
            markets = await self._fetch_markets_fallback(session)

        return markets

    async def _fetch_markets_fallback(self, session: aiohttp.ClientSession) -> list[dict]:
        """Fallback: Henter direkte via /markets med sports tag."""
        markets = []
        try:
            url = f"{self.base_url}/markets"
            params = {
                "active":   "true",
                "closed":   "false",
                "limit":    500,
                "tag_slug": "sports",
            }
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

            raw = data if isinstance(data, list) else data.get("markets", [])
            for m in raw:
                token_ids = _extract_token_ids(m)
                if token_ids:
                    m["_token_ids"] = token_ids
                    markets.append(m)

            log.info(f"Fallback: {len(markets)} markeder fra /markets endpoint")
        except Exception as e:
            log.error(f"Markets fallback fejl: {e}")
        return markets

    def extract_all_token_ids(self, markets: list[dict]) -> list[str]:
        """Returnerer alle unikke token IDs fra en liste af markeder."""
        ids = set()
        for m in markets:
            for tid in _extract_token_ids(m):
                ids.add(tid)
        return list(ids)

    def classify_market(self, market: dict) -> str:
        """
        Klassificerer et market baseret på question/slug.
        Returnerer en kort type-streng til brug i analyse.
        """
        q = (market.get("question") or market.get("_event_title") or "").lower()

        if "both teams" in q or "btts" in q or "both score" in q:
            return "both_teams_score"
        elif "over 0.5" in q:
            return "over_0.5"
        elif "over 1.5" in q:
            return "over_1.5"
        elif "over 2.5" in q:
            return "over_2.5"
        elif "over 3.5" in q:
            return "over_3.5"
        elif "half" in q and ("first" in q or "1st" in q):
            return "first_half"
        elif "half" in q and ("second" in q or "2nd" in q):
            return "second_half"
        elif "clean sheet" in q:
            return "clean_sheet"
        elif "win" in q or "winner" in q or "beat" in q:
            return "winner"
        elif "draw" in q:
            return "draw"
        elif "score" in q and any(f"{i}-{j}" in q for i in range(6) for j in range(6)):
            return "exact_score"
        elif "yellow" in q or "card" in q:
            return "cards"
        elif "corner" in q:
            return "corners"
        else:
            return "other"
