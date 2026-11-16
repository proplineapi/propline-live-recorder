"""Record one live game from PropLine to a JSONL file.

    python recorder.py record --sport basketball_nba --event-id 123456 --out game.jsonl
    python recorder.py replay game.jsonl --speed 20

What a recording holds, one JSON object per line, each with a "kind":

  pregame_board   every book's player props for the game (from /odds)
  event           every stream frame for the game, in sequence order:
                  stat_update (live stat changes + period + clock), play
                  (NBA play-by-play), game_state (score, period, clock)
  final_stats     the official box score (/stats)
  results         every prop graded won / lost / push / void (/results)

Props, stats and plays all carry the same `player_id`, so a prop joins its
player's live and final stats without matching names.

Needs a Streaming Lite (or higher) key for the websocket stream.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
import websockets

API = os.environ.get("PROPLINE_API_BASE", "https://api.prop-line.com/v1")
WS = os.environ.get("PROPLINE_WS_URL", "wss://ws.prop-line.com/v1/stream")

PROP_MARKETS = {
    "basketball_nba": [
        "player_points", "player_rebounds", "player_assists", "player_threes",
        "player_steals", "player_blocks", "player_turnovers",
        "player_points_rebounds_assists", "player_points_rebounds",
        "player_points_assists", "player_rebounds_assists",
        "player_double_double",
    ],
    "basketball_wnba": [
        "player_points", "player_rebounds", "player_assists", "player_threes",
        "player_points_rebounds_assists",
    ],
    "baseball_mlb": [
        "pitcher_strikeouts", "pitcher_outs", "batter_hits", "batter_total_bases",
        "batter_home_runs", "batter_rbis", "batter_hits_runs_rbis",
    ],
    "hockey_nhl": [
        "player_points", "player_goals", "player_assists", "player_shots_on_goal",
        "goalie_saves",
    ],
    "football_nfl": [
        "player_pass_yds", "player_rush_yds", "player_reception_yds",
        "player_receptions", "player_anytime_td",
    ],
}

STREAM_EVENTS = ["stat_update", "play", "game_state"]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Client:
    def __init__(self, key: str):
        self.s = requests.Session()
        self.s.headers["X-API-Key"] = key

    def get(self, path: str, **params):
        r = self.s.get(f"{API}{path}", params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: dict):
        r = self.s.post(f"{API}{path}", json=body, timeout=60)
        r.raise_for_status()
        return r.json()

    def delete(self, path: str):
        self.s.delete(f"{API}{path}", timeout=60)


def write(out, kind: str, data, **extra) -> None:
    out.write(json.dumps({"kind": kind, "recorded_at": now(), **extra, "data": data}) + "\n")
    out.flush()


def snapshot(client: Client, out, sport: str, event_id: int, final: bool) -> None:
    base = f"/sports/{sport}/events/{event_id}"
    if not final:
        markets = ",".join(PROP_MARKETS.get(sport, []))
        board = client.get(f"{base}/odds", markets=markets) if markets else client.get(f"{base}/odds")
        write(out, "pregame_board", board)
        return
    write(out, "final_stats", client.get(f"{base}/stats"))
    try:
        write(out, "results", client.get(f"{base}/results"))
    except requests.HTTPError as e:
        print(f"results not available yet ({e}); fetch later with /results", file=sys.stderr)


async def stream(key: str, webhook_id: int, out, since_seq: int, stop: asyncio.Event) -> None:
    """Follow the stream until `stop` is set. Reconnects with the last seq,
    so a dropped socket loses nothing."""
    while not stop.is_set():
        try:
            async with websockets.connect(WS, max_size=None, ping_interval=None) as ws:
                await ws.send(json.dumps({
                    "type": "auth", "api_key": key,
                    "webhook_id": webhook_id, "since_seq": since_seq,
                }))
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        continue
                    msg = json.loads(raw)
                    t = msg.get("type")
                    if t == "ready":
                        print(f"stream connected (latest seq {msg.get('latest_seq')})", file=sys.stderr)
                        if msg.get("truncated"):
                            print("warning: events aged out before we reconnected", file=sys.stderr)
                    if t != "event":
                        continue
                    since_seq = max(since_seq, msg["seq"])
                    write(out, "event", msg["data"], seq=msg["seq"],
                          event_type=msg["event_type"], created_at=msg.get("created_at"))
        except (OSError, websockets.ConnectionClosed) as e:
            if stop.is_set():
                return
            print(f"stream dropped ({e}); reconnecting from seq {since_seq}", file=sys.stderr)
            await asyncio.sleep(3)


async def watch_final(client: Client, sport: str, event_id: int,
                      after_final: float, stop: asyncio.Event) -> None:
    """Set `stop` once /scores says the game is final, plus a grace period
    for the last stat corrections."""
    while True:
        try:
            rows = await asyncio.to_thread(client.get, f"/sports/{sport}/scores")
            row = next((r for r in rows if str(r.get("id")) == str(event_id)), None)
            if row and row.get("status") in ("final", "cancelled", "postponed"):
                print(f"game {row['status']}; stopping in {after_final:.0f}s", file=sys.stderr)
                await asyncio.sleep(after_final)
                stop.set()
                return
        except Exception as e:  # keep recording through a failed poll
            print(f"/scores poll failed: {e}", file=sys.stderr)
        await asyncio.sleep(60)


async def follow(client: Client, key: str, webhook_id: int, out, sport: str,
                 event_id: int, after_final: float) -> None:
    stop = asyncio.Event()
    await asyncio.gather(
        stream(key, webhook_id, out, 0, stop),
        watch_final(client, sport, event_id, after_final, stop),
    )


def record(args) -> None:
    key = args.api_key or os.environ.get("PROPLINE_API_KEY")
    if not key:
        sys.exit("set PROPLINE_API_KEY or pass --api-key")
    client = Client(key)
    sub = client.post("/webhooks", {
        "transport": "websocket",
        "events": STREAM_EVENTS,
        "filter_sport_key": args.sport,
        "filter_event_id": args.event_id,
    })
    webhook_id = sub["id"]
    print(f"subscription {webhook_id} created; recording to {args.out}", file=sys.stderr)
    try:
        with open(args.out, "a") as out:
            write(out, "meta", {"sport": args.sport, "event_id": args.event_id,
                                "webhook_id": webhook_id, "events": STREAM_EVENTS})
            snapshot(client, out, args.sport, args.event_id, final=False)
            try:
                asyncio.run(follow(client, key, webhook_id, out, args.sport,
                                   args.event_id, args.after_final))
            except KeyboardInterrupt:
                print("stopped early; what was recorded is kept", file=sys.stderr)
                return
            snapshot(client, out, args.sport, args.event_id, final=True)
    finally:
        if not args.keep_subscription:
            client.delete(f"/webhooks/{webhook_id}")
    print("done", file=sys.stderr)


def replay(args) -> None:
    """Print a recording's stream events in order, paced by their own timestamps."""
    rows = [json.loads(line) for line in open(args.file)]
    events = sorted((r for r in rows if r["kind"] == "event"), key=lambda r: r["seq"])
    prev = None
    for r in events:
        t = datetime.fromisoformat(r["created_at"]) if r.get("created_at") else None
        if prev and t and args.speed > 0:
            time.sleep(max(0.0, (t - prev).total_seconds()) / args.speed)
        prev = t or prev
        d = r["data"]
        if r["event_type"] == "stat_update":
            for c in d.get("changes", []):
                print(f"Q{d.get('period')} {d.get('clock')}  {c['player_name']} "
                      f"{c['stat_type']} {c['previous']} -> {c['current']}  ({c.get('player_id')})")
        elif r["event_type"] == "play":
            print(f"Q{d.get('period')} {d.get('clock')}  {d.get('description')}")
        elif r["event_type"] == "game_state":
            print(f"[{d.get('status')}] {d.get('away_team')} {d.get('away_score')} - "
                  f"{d.get('home_score')} {d.get('home_team')}  {d.get('period')} {d.get('clock')}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sp = p.add_subparsers(dest="cmd", required=True)
    r = sp.add_parser("record", help="record one live game")
    r.add_argument("--sport", default="basketball_nba")
    r.add_argument("--event-id", type=int, required=True)
    r.add_argument("--out", default="game.jsonl")
    r.add_argument("--api-key")
    r.add_argument("--after-final", type=float, default=600.0,
                   help="seconds to keep listening after the game goes final")
    r.add_argument("--keep-subscription", action="store_true")
    r.set_defaults(fn=record)
    q = sp.add_parser("replay", help="print a recording in order")
    q.add_argument("file")
    q.add_argument("--speed", type=float, default=0.0,
                   help="0 = as fast as possible; 20 = 20x real time")
    q.set_defaults(fn=replay)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
