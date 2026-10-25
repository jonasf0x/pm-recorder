"""Wide recorder: the reward pools ranked below record.py's 60, at light fidelity (research/rewards/FINDINGS.md §8).

record.py follows the 60 biggest liquidity-reward pools every minute. Europa's rewards were still rising at 60
pools, and ~5,000 pools are quotable, so this follows the next ones (ranks FIRST+1 … LAST by daily rate, mid 10-90c,
ending >= 2 days out) every STEP seconds (5 minutes): the books within the reward zone and the taker trades since
the previous sample, in the same per-sample format as record.py's minutes. Nothing goes to Supabase: each hour's
bundle is written to rec/ and uploaded as the run's artifact. The overlap with record.py's ranks (FIRST = 40)
covers rank changes at the boundary. Runs chain like record.py (seconds, handoff, next); both runs of an overlap
record, readers drop samples they already have (same t within 2 minutes).

  python wide.py 21000 17100 21000,17100
"""
import gzip
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

import record
from record import books, iso, scan, trades, utc

FIRST, LAST = 40, 500            # ranks by daily reward rate: record.py takes 1-60
STEP = 300                       # seconds between samples
OUT = record.OUT

lock = threading.Lock()
wide_set = []                    # [{cid, tok, v, mos, rate}]
wide_markets = {}                # cid -> the market's compact scan entry (tick, end, question, event)
buf = []                         # samples since the last flush


def pick_wide(ms, now):
    """Ranks FIRST+1 … LAST of the reward pools (same filters as record.pick_rewards)."""
    ok = sorted([m for m in ms if m["rw"] and m["rw"][0] > 0 and m["rw"][1] and m["gb"] is not None and m["ga"] is not None
                 and 0.10 <= (m["gb"] + m["ga"]) / 2 <= 0.90
                 and datetime.fromisoformat(m["end"].replace("Z", "+00:00")) >= now + timedelta(days=2)],
                key=lambda m: -m["rw"][0])
    return ok[FIRST:LAST]


def refresh():
    """Hourly: rescan the market list and re-pick the wide set."""
    now = utc()
    ms = pick_wide(scan(now), now)
    rows = [{"cid": m["cid"], "tok": m["tok"], "v": m["rw"][1] / 100, "mos": m["rw"][2] or 0, "rate": m["rw"][0]} for m in ms]
    with lock:
        wide_set[:] = rows
        for m in ms:
            wide_markets[m["cid"]] = {k: m[k] for k in ("cid", "tok", "no", "q", "ev", "end", "tick", "mos", "ver", "fee", "rw") if k in m}
    print(f"{iso(now)}: wide set {len(rows)} pools, ${sum(r['rate'] for r in rows):,.0f}/day", flush=True)


def sample_loop(stop):
    last = utc().timestamp() - STEP
    while not stop.is_set():
        nxt = (int(time.time()) // STEP + 1) * STEP + 5
        stop.wait(max(nxt - time.time(), 0))
        if stop.is_set():
            break
        with lock:
            rs = list(wide_set)
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
                buf.append(rec)
        except Exception as e:                                   # keep recording through API hiccups
            print("sample failed:", e, file=sys.stderr, flush=True)


def flush(hour):
    with lock:
        samples = list(buf)
        buf.clear()
        rows = list(wide_set)
        mk = [wide_markets[r["cid"]] for r in rows if r["cid"] in wide_markets]
    if not samples:
        return
    bundle = {"v": 1, "wide": True, "step": STEP, "hour": hour, "ts": iso(utc()), "markets": mk, "reward_set": rows, "minutes": samples}
    raw = gzip.compress(json.dumps(bundle, separators=(",", ":")).encode(), 6)
    OUT.mkdir(exist_ok=True)
    (OUT / f"wide_{hour}.json.gz").write_bytes(raw)
    print(f"{hour}: {len(samples)} samples, {len(rows)} pools, {len(raw) / 1e6:.1f} MB", flush=True)


def handoff(nxt):
    s, h = nxt.split(",")
    os.system(f'gh workflow run wide.yml -f seconds={s} -f handoff={h} -f next={nxt}')


def main():
    seconds, hand, nxt = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    start = time.time()
    stop = threading.Event()
    try:
        refresh()
    except Exception as e:
        print("first scan failed:", e, file=sys.stderr, flush=True)
    th = threading.Thread(target=sample_loop, args=(stop,), daemon=True)
    th.start()
    handed, hour, last_refresh = False, utc().strftime("%Y-%m-%dT%H"), time.time()
    while time.time() - start < seconds:
        if hand and not handed and time.time() - start >= hand:
            handoff(nxt)
            handed = True
        h = utc().strftime("%Y-%m-%dT%H")
        if h != hour:
            flush(hour)
            hour = h
        if time.time() - last_refresh >= 3600:
            try:
                refresh()
                last_refresh = time.time()
            except Exception as e:
                print("rescan failed:", e, file=sys.stderr, flush=True)
                last_refresh = time.time() - 3000              # retry in ~10 minutes
        time.sleep(15)
    stop.set()
    th.join(timeout=120)
    flush(hour)


if __name__ == "__main__":
    main()
