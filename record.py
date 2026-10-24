"""Records public Polymarket market data on GitHub Actions (no keys for Polymarket; read-only).

Every hour (at :30 UTC) a snapshot:
  - every accepting market that ends within 4h-45 days (Gamma events keyset, tags included),
  - the order book (CLOB /books) of the non-recurring ones whose Yes ask is 2-45c and that are not
    sports/esports or were created in the last 72 hours,
  - the taker trades (Data API v2) of the previous snapshot's book markets since that snapshot.
Every minute: the books and trades of the 60 markets with the biggest liquidity-reward pools
(mid 10-90c, ending >= 2 days out).

Each hour's bundle (gzip JSON) goes to Supabase Storage (bucket pm-recorder, last 72 hours) and to
the run's artifact. Runs chain like hl-recorder: each records `seconds` and starts its successor after
`handoff` seconds — an hour of overlap, because GitHub can take that long to start a run. Both record
minutes during the overlap; only the older one takes the hourly snapshots (the newer waits until the older
has finished, then its first bundle also carries the overlap's minutes; readers skip minutes they already
have). Until 2026-10-24 the old run stopped snapshotting at the handoff and the gap until the successor
started (15-58 min, every ~6h) lost its minutes.

  python record.py 21000 17100 21000,17100
"""
import gzip
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events/keyset"
BOOKS = "https://clob.polymarket.com/books"
TRADES = "https://data-api.polymarket.com/v2/trades"
SB_URL, SB_KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SECRET_KEY")
BUCKET, KEEP_H = "pm-recorder", 72
OUT = Path("rec")
SLOT_MIN = 30                 # hourly snapshot at :30
N_REWARD = 60                 # markets recorded every minute
RUN_S, HANDOFF_S = 21000, 17100   # 5h50 a run (job timeout 5h58), successor started at 4h45
OLD_TIMES = (21000, 20400)        # what runs started before 2026-10-24 pass on: read as the new times
RECURRING = re.compile(r"up or down|dip to|reach \$|price of .* (?:be|above|between)|highest temperature|"
                       r"# of tweets|tweets .* (?:from|between)|truth social posts", re.I)
SPORTS = {"sports", "games", "esports"}

S = requests.Session()
S.headers["User-Agent"] = "pm-recorder"
lock = threading.Lock()
minute_buf = []               # per-minute reward-market records since the last snapshot
reward_set = []               # [{cid, tok, v, mos, rate}]


def utc():
    return datetime.now(timezone.utc)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def get(url, params=None, tries=5):
    for k in range(tries):
        try:
            r = S.get(url, params=params, timeout=60)
            if r.status_code in (404, 429) or r.status_code >= 500:   # Gamma's keyset 404s now and then
                raise requests.RequestException(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError):
            if k == tries - 1:
                raise
            time.sleep(2 * (k + 1))


def post(url, payload, tries=5):
    for k in range(tries):
        try:
            r = S.post(url, json=payload, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.RequestException(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError):
            if k == tries - 1:
                raise
            time.sleep(2 * (k + 1))


def num(x):
    try:
        return float(x) if x not in (None, "") else None
    except (TypeError, ValueError):
        return None


def ids(m):
    """(yes, no) token ids, chosen by the market's version (Protocol V2: positionIds)."""
    try:
        if (m.get("version") or "v1").lower() == "v2":
            t = m.get("positionIds") or []
        else:
            t = json.loads(m.get("clobTokenIds") or "[]")
    except (json.JSONDecodeError, TypeError):
        return None
    return (str(t[0]), str(t[1])) if len(t) == 2 else None


def scan(now):
    """Every accepting binary market ending within 4h-45d, compact, with its event's tags."""
    p = dict(active="true", closed="false", limit=100, include_tag="true",
             end_date_min=iso(now + timedelta(hours=4)), end_date_max=iso(now + timedelta(days=45)))
    out, cur = [], None
    for _ in range(400):
        q = dict(p) | ({"after_cursor": cur} if cur else {})
        d = get(GAMMA_EVENTS, q)
        for e in d.get("events") or []:
            tags = [t.get("slug") for t in e.get("tags") or [] if t.get("slug")]
            for m in e.get("markets") or []:
                if m.get("closed") or not (m.get("acceptingOrders") and m.get("enableOrderBook")):
                    continue
                tk = ids(m)
                try:
                    outcomes = json.loads(m.get("outcomes") or "[]")
                except (json.JSONDecodeError, TypeError):
                    outcomes = []
                if not tk or len(outcomes) != 2 or not m.get("conditionId") or not m.get("endDate"):
                    continue
                rw = m.get("clobRewards") or []
                out.append({
                    "cid": m["conditionId"], "tok": tk[0], "no": tk[1], "q": (m.get("question") or "")[:140],
                    "ev": e.get("slug") or "", "tags": tags[:8], "end": m["endDate"],
                    "st": m.get("gameStartTime") or m.get("eventStartTime") or e.get("startTime"),
                    "cr": m.get("createdAt"), "vol": num(m.get("volume")) or 0.0, "v24": num(m.get("volume24hr")) or 0.0,
                    "neg": (m.get("negRiskMarketID") or e.get("negRiskMarketID") or e.get("slug")) if m.get("negRisk") else None,
                    "tick": num(m.get("orderPriceMinTickSize")) or 0.01, "mos": num(m.get("orderMinSize")) or 5.0,
                    "ver": (m.get("version") or "v1").lower(), "combo": m.get("comboStatus"), "rfq": bool(m.get("rfqEnabled")),
                    "fee": num((m.get("feeSchedule") or {}).get("rate")) or 0.0,
                    "rw": [sum(num(x.get("rewardsDailyRate")) or 0 for x in rw), num(m.get("rewardsMaxSpread")),
                           num(m.get("rewardsMinSize"))] if rw else None,
                    "gb": num(m.get("bestBid")), "ga": num(m.get("bestAsk")),
                })
        cur = d.get("next_cursor")
        if not cur or not d.get("events"):
            break
    return out


def age_h(m, now):
    try:
        return (now - datetime.fromisoformat(m["cr"].replace("Z", "+00:00"))).total_seconds() / 3600
    except (AttributeError, TypeError, ValueError):
        return 1e9


def recurring(m):
    return "recurring" in m["tags"] or bool(RECURRING.search(m["q"]))


def book_set(ms, now):
    return [m for m in ms if m["ga"] is not None and 0.02 <= m["ga"] <= 0.45 and not recurring(m)
            and (not SPORTS & set(m["tags"]) or age_h(m, now) <= 72)]


def books(tokens):
    """asset_id -> {bids: [[p, s]...] best first, asks: [...]}, batches of 100."""
    out = {}

    def one(chunk):
        try:
            return post(BOOKS, [{"token_id": t} for t in chunk])
        except requests.RequestException as e:
            print("books failed:", e, file=sys.stderr)
            return []
    chunks = [tokens[i:i + 100] for i in range(0, len(tokens), 100)]
    with ThreadPoolExecutor(4) as ex:
        for res in ex.map(one, chunks):
            for b in res or []:
                bids = sorted(((float(l["price"]), float(l["size"])) for l in b.get("bids", [])), reverse=True)
                asks = sorted((float(l["price"]), float(l["size"])) for l in b.get("asks", []))
                out[str(b.get("asset_id"))] = {"bids": bids, "asks": asks}
    return out


def trades(cids, since, until=None):
    """cid -> [[ts, yes_buy (0/1), yes_price, shares]] for taker trades in (since, until] (epoch s)."""
    out = {}

    def one(chunk):
        rows, cur = [], None
        for _ in range(30):
            p = {"condition": ",".join(chunk), "limit": 500} | ({"cursor": cur} if cur else {})
            try:
                d = get(TRADES, p)
            except requests.RequestException as e:
                print("trades failed:", e, file=sys.stderr)
                return rows, False
            data = d.get("data") or []
            rows += data
            cur = (d.get("pagination") or {}).get("next_cursor")
            if not cur or not data or min(t["timestamp"] for t in data) <= since:
                break
        return rows, True
    chunks = [cids[i:i + 20] for i in range(0, len(cids), 20)]
    ok = True
    with ThreadPoolExecutor(4) as ex:
        for rows, good in ex.map(one, chunks):
            ok &= good
            for t in rows:
                if t["timestamp"] <= since or (until is not None and t["timestamp"] > until):
                    continue
                yes = t.get("outcome_index") == 0
                px = float(t["price"])
                out.setdefault(t["condition_id"], []).append(
                    [t["timestamp"], int((t.get("side") == "BUY") == yes), round(px if yes else 1 - px, 4),
                     round(float(t.get("size") or 0), 2)])
    for v in out.values():
        v.sort()
    return out, ok


def compact_book(b, n=30):
    """Best bid/ask, size there, size within 5c, and the first n ask levels (deep asks need the size resting at 2x the ask)."""
    if not b:
        return None
    bids, asks = b["bids"], b["asks"]
    bb, ba = (bids[0][0] if bids else None), (asks[0][0] if asks else None)
    return {"bid": bb, "bsz": round(bids[0][1], 2) if bids else 0, "ask": ba, "asz": round(asks[0][1], 2) if asks else 0,
            "bd5": round(sum(s for p, s in bids if bb is not None and p >= bb - 0.05), 2),
            "ad5": round(sum(s for p, s in asks if ba is not None and p <= ba + 0.05), 2),
            "asks": [[p, round(s, 2)] for p, s in asks[:n]]}


def pick_rewards(ms, now):
    """The N_REWARD biggest daily reward pools with the mid in 10-90c, ending >= 2 days out."""
    return sorted([m for m in ms if m["rw"] and m["rw"][0] > 0 and m["rw"][1] and m["gb"] is not None and m["ga"] is not None
                   and 0.10 <= (m["gb"] + m["ga"]) / 2 <= 0.90
                   and datetime.fromisoformat(m["end"].replace("Z", "+00:00")) >= now + timedelta(days=2)],
                  key=lambda m: -m["rw"][0])[:N_REWARD]


def reward_rows(rewards):
    return [{"cid": m["cid"], "tok": m["tok"], "v": m["rw"][1] / 100, "mos": m["rw"][2] or 0, "rate": m["rw"][0]}
            for m in rewards]


# ------------------------------------------------------------------ storage

def sb(method, path, **kw):
    h = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"} | kw.pop("headers", {})
    for k in range(4):
        try:
            r = requests.request(method, f"{SB_URL}/storage/v1/{path}", headers=h, timeout=120, **kw)
            r.raise_for_status()
            return r
        except requests.RequestException:
            if k == 3:
                raise
            time.sleep(5 * (k + 1))


def put(name, data):
    OUT.mkdir(exist_ok=True)
    (OUT / name.replace("/", "_")).write_bytes(data)
    if SB_URL and SB_KEY:
        sb("POST", f"object/{BUCKET}/{name}", data=data,
           headers={"Content-Type": "application/gzip", "x-upsert": "true"})


def latest():
    if not (SB_URL and SB_KEY):
        return None
    try:
        return json.loads(gzip.decompress(sb("GET", f"object/{BUCKET}/latest.json.gz").content))
    except requests.RequestException:
        return None


def prune(now):
    if not (SB_URL and SB_KEY):
        return
    names = [x["name"] for x in sb("POST", f"object/list/{BUCKET}", json={"prefix": "hour/", "limit": 1000}).json()]
    cut = (now - timedelta(hours=KEEP_H)).strftime("%Y-%m-%dT%H")
    old = [f"hour/{n}" for n in names if n[:13] < cut]
    if old:
        sb("DELETE", f"object/{BUCKET}", json={"prefixes": old})


# ------------------------------------------------------------------ loops

def snapshot(slot):
    t0 = utc()
    prev = latest()
    if prev and prev["slot"] >= iso(slot):
        print(f"{iso(slot)} already recorded", flush=True)
        return
    ms = scan(t0)
    bs = book_set(ms, t0)
    bk = books([m["tok"] for m in bs])
    ts = utc()
    since = max(datetime.fromisoformat(prev["ts"].replace("Z", "+00:00")).timestamp(), ts.timestamp() - 3 * 3600) \
        if prev else ts.timestamp() - 3600
    tr, ok = trades(prev["cids"], since, ts.timestamp()) if prev else ({}, True)
    for m in bs:
        m["b"] = compact_book(bk.get(m["tok"]))
    rewards = pick_rewards(ms, ts)
    with lock:
        mins = list(minute_buf)
        minute_buf.clear()
        reward_set[:] = reward_rows(rewards)
    keep = {m["cid"] for m in bs} | {m["cid"] for m in rewards}
    bundle = {"v": 1, "slot": iso(slot), "ts": iso(ts), "since": since, "trades_ok": ok,
              "markets": [m for m in ms if m["cid"] in keep], "n_scanned": len(ms),
              "trades": tr, "minutes": mins, "reward_set": reward_set}
    raw = gzip.compress(json.dumps(bundle, separators=(",", ":")).encode(), 6)
    name = f"hour/{slot.strftime('%Y-%m-%dT%H')}.json.gz"
    put(name, raw)
    put("latest.json.gz", gzip.compress(json.dumps({"slot": iso(slot), "ts": iso(ts), "file": name,
                                                    "cids": [m["cid"] for m in bs]}).encode()))
    prune(ts)
    print(f"{iso(slot)}: {len(ms)} markets, {len(bs)} books, {sum(map(len, tr.values()))} trades, "
          f"{len(mins)} minutes, {len(raw) / 1e6:.1f} MB, {(utc() - t0).total_seconds():.0f}s", flush=True)


def minute_loop(stop):
    last = utc().timestamp() - 60
    while not stop.is_set():
        nxt = (int(time.time()) // 60 + 1) * 60 + 5
        stop.wait(max(nxt - time.time(), 0))
        if stop.is_set():
            break
        with lock:
            rs = list(reward_set)
        if not rs:
            continue
        try:
            t = utc().timestamp()
            bk = books([r["tok"] for r in rs])
            tr, _ = trades([r["cid"] for r in rs], last)
            rec = {"t": int(t), "m": {}}
            for r in rs:
                b = bk.get(r["tok"])
                if not b or not b["bids"] or not b["asks"]:
                    continue
                mid = (b["bids"][0][0] + b["asks"][0][0]) / 2
                w = r["v"] + 0.01
                rec["m"][r["cid"]] = {"b": [[p, round(s, 2)] for p, s in b["bids"] if p >= mid - w],
                                      "a": [[p, round(s, 2)] for p, s in b["asks"] if p <= mid + w],
                                      "t": tr.get(r["cid"], [])}
            last = t
            with lock:
                minute_buf.append(rec)
        except Exception as e:                                   # keep recording through API hiccups
            print("minute failed:", e, file=sys.stderr, flush=True)


def handoff(nxt):
    s, h = nxt.split(",")
    if (int(s), int(h)) == OLD_TIMES:
        s, h, nxt = RUN_S, HANDOFF_S, f"{RUN_S},{HANDOFF_S}"
    os.system(f'gh workflow run record.yml -f seconds={s} -f handoff={h} -f next={nxt}')


_older = {"t": 0.0, "v": False}


def older_running():
    """Is an older recorder run still in progress (and young enough to be alive)? Checked every 5 minutes;
    on any error, False (snapshot rather than risk a gap)."""
    if time.time() - _older["t"] < 300:
        return _older["v"]
    me, v = int(os.getenv("GITHUB_RUN_ID", "0") or 0), False
    try:
        out = os.popen("gh run list --workflow record.yml --status in_progress --json databaseId,startedAt").read()
        for r in json.loads(out or "[]"):
            age = (utc() - datetime.fromisoformat(r["startedAt"].replace("Z", "+00:00"))).total_seconds()
            if me and r["databaseId"] < me and age < RUN_S + 600:
                v = True
    except Exception as e:
        print("older-run check failed:", e, file=sys.stderr, flush=True)
    _older.update(t=time.time(), v=v)
    return v


def main():
    seconds, hand, nxt = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    if (seconds, hand) == OLD_TIMES:                             # started by a run from before the change
        seconds, hand, nxt = RUN_S, HANDOFF_S, f"{RUN_S},{HANDOFF_S}"
    if not (SB_URL and SB_KEY):                                  # the trade windows chain through storage
        print("SUPABASE_URL / SUPABASE_SECRET_KEY secrets not set: nothing recorded")
        return
    start = time.time()
    stop = threading.Event()
    # reward set before the first snapshot: one scan now
    try:
        rs = reward_rows(pick_rewards(scan(utc()), utc()))
        with lock:
            reward_set[:] = rs
    except Exception as e:
        print("first scan failed:", e, file=sys.stderr)
    th = threading.Thread(target=minute_loop, args=(stop,), daemon=True)
    th.start()
    handed, done = False, None
    while time.time() - start < seconds:
        if hand and not handed and time.time() - start >= hand:
            handoff(nxt)
            handed = True
        now = utc()
        slot = now.replace(minute=SLOT_MIN, second=0, microsecond=0)
        if slot <= now < slot + timedelta(minutes=20) and done != slot and not older_running():
            try:
                snapshot(slot)
                done = slot
            except Exception as e:                            # retried until the slot's 20 minutes are up
                print(f"snapshot {iso(slot)} failed: {e}", file=sys.stderr, flush=True)
                time.sleep(40)
        time.sleep(20)
    stop.set()
    th.join(timeout=90)


if __name__ == "__main__":
    main()
