# propline-live-recorder

Record one live game from the [PropLine API](https://prop-line.com) to a file, then replay it in order.

A recording holds everything you need to build or test a live app:

| kind | what it is | source |
|---|---|---|
| `pregame_board` | every book's player props for the game | `GET /odds?markets=...` |
| `event` / `stat_update` | each live stat change, with period + clock | websocket stream |
| `event` / `play` | NBA play-by-play | websocket stream |
| `event` / `game_state` | score, period, clock | websocket stream |
| `final_stats` | the official box score | `GET /stats` |
| `results` | every prop graded won / lost / push / void | `GET /results` |

Props, stats and plays all carry the same `player_id` (e.g. `nba:1628369`), so a prop joins its player's live and final stats without matching names.

## Use it

Needs a Streaming Lite (or higher) key for the stream. A free key still gets the pregame board and the final box score.

```bash
pip install -r requirements.txt
export PROPLINE_API_KEY=your_key

# find tonight's game ids
curl "https://api.prop-line.com/v1/sports/basketball_nba/events?apiKey=$PROPLINE_API_KEY"

# start before tip-off; it stops on its own 10 minutes after the final
python recorder.py record --sport basketball_nba --event-id 691534 --out game.jsonl

# replay it at 20x
python recorder.py replay game.jsonl --speed 20
```

Replay output looks like:

```
Q1 10:48  J. Tatum 3PT (3 PTS) (J. Brown 1 AST)
Q1 10:48  Jayson Tatum points 0.0 -> 3.0  (nba:1628369)
[in_progress] Orlando Magic 0 - 3 Boston Celtics  Q1 10:48
```

## How it works

1. Creates a websocket subscription filtered to the one game (`stat_update`, `play`, `game_state`) and deletes it at the end.
2. Saves the pregame prop board.
3. Writes every stream frame to the file in `seq` order. If the socket drops it reconnects with the last `seq`, so nothing is lost (the server keeps 30 minutes for resume).
4. Polls `/scores` once a minute. When the game is final it waits 10 minutes, then saves the final box score and the graded props.

Want line movement on the props too? Add `"line_movement"` to `STREAM_EVENTS` (it is a lot more traffic).

## Samples

Three full recorded NBA games, tip-off to the final buzzer. Each holds the pregame board (every book's odds), every play, stat update and game-state update, the final box score and every graded prop.

| File | Game | Lines | Plays | Stat updates (player stat changes) | Game-state updates |
|---|---|---|---|---|---|
| [nba-2026-11-16-orl-at-bos.jsonl.gz](samples/nba-2026-11-16-orl-at-bos.jsonl.gz) | Orlando Magic 115, Boston Celtics 105 (Nov 16, 2026) | 1,525 | 823 | 340 (4,693) | 358 |
| [nba-2026-11-17-nyk-at-tor.jsonl.gz](samples/nba-2026-11-17-nyk-at-tor.jsonl.gz) | New York Knicks 111, Toronto Raptors 102 (Nov 17, 2026) | 1,623 | 966 | 305 (4,150) | 348 |
| [nba-2026-11-17-cha-at-lal.jsonl.gz](samples/nba-2026-11-17-cha-at-lal.jsonl.gz) | Los Angeles Lakers 118, Charlotte Hornets 114 (Nov 17, 2026) | 1,720 | 929 | 377 (5,095) | 410 |

```bash
gunzip -k samples/nba-2026-11-17-cha-at-lal.jsonl.gz
python recorder.py replay samples/nba-2026-11-17-cha-at-lal.jsonl
```

## Docs

- [API docs](https://prop-line.com/docs)
- [Recipes](https://prop-line.com/recipes)

MIT licensed.
