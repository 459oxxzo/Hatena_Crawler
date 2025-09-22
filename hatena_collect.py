# -*- coding: utf-8 -*-
import os, re, csv, time, urllib.parse, datetime as dt, pathlib, sys
import requests

ENTRY = "https://b.hatena.ne.jp/entry/jsonlite/?url="
STAR  = "https://s.hatena.ne.jp/entry.json?uri="

def build_permalink(user, timestamp, eid):
    ymd = re.sub(r"[^\d]", "", (timestamp or ""))[:8]
    return f"https://b.hatena.ne.jp/{user}/{ymd}#bookmark-{eid}"

def fetch_json(url, timeout=20):
    r = requests.get(url, timeout=timeout, headers={"User-Agent":"hatena-poc-collector"})
    r.raise_for_status()
    return r.json()

def collect_one(entry_url):
    ej = fetch_json(ENTRY + urllib.parse.quote(entry_url, safe=""))
    eid   = ej.get("eid")
    title = ej.get("title","")
    comments, stars_rows = [], []
    today = dt.date.today().isoformat()

    for bm in ej.get("bookmarks", []):
        user = bm.get("user")
        comment = (bm.get("comment") or "").strip()
        ts = bm.get("timestamp") or bm.get("created_datetime") or ""
        pl = build_permalink(user, ts, eid)

        sj = fetch_json(STAR + urllib.parse.quote(pl, safe=""))
        stars = []
        for ent in sj.get("entries", []):
            for s in ent.get("stars", []):
                name = s.get("name") or s.get("user")
                if name:
                    stars.append({"user": name, "color": s.get("color")})

        comments.append({
            "permalink": pl,
            "comment_user": user,
            "timestamp": ts,
            "comment_text": comment,
            "star_count": len(stars),
            "entry_url": entry_url,
            "eid": eid,
            "title": title,
        })

        from collections import Counter
        cnt = Counter((st["user"], st.get("color")) for st in stars)
        for (giver, color), c in cnt.items():
            stars_rows.append({
                "permalink": pl,
                "giver_user": giver,
                "color": color if color is not None else "",
                "snapshot_date": today,
                "count": c,
            })
        time.sleep(0.15)  # polite

    return comments, stars_rows

def main():
    seeds_path = os.getenv("SEEDS_FILE", "seeds.txt")
    out_root   = os.getenv("OUT_DIR", "out")
    date_dir   = dt.date.today().isoformat()

    urls = [ln.strip() for ln in open(seeds_path, encoding="utf-8") if ln.strip() and not ln.startswith("#")]
    outdir = pathlib.Path(out_root) / date_dir
    outdir.mkdir(parents=True, exist_ok=True)

    all_comments, all_stars = [], []
    for u in urls:
        try:
            cm, st = collect_one(u)
            all_comments.extend(cm)
            all_stars.extend(st)
        except Exception as e:
            print("ERROR:", u, e, file=sys.stderr)

    com_cols = ["permalink","comment_user","timestamp","comment_text","star_count","entry_url","eid","title"]
    with open(outdir / "comments.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=com_cols); w.writeheader(); w.writerows(all_comments)

    star_cols = ["permalink","giver_user","color","snapshot_date","count"]
    with open(outdir / "stars_snapshot.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=star_cols); w.writeheader(); w.writerows(all_stars)

    # 簡易整合チェック
    from collections import Counter
    snap = Counter([r["permalink"] for r in all_stars])
    diff = []
    for r in all_comments:
        if snap.get(r["permalink"], 0) != r["star_count"]:
            diff.append((r["permalink"], r["star_count"], snap.get(r["permalink"],0)))
    with open(outdir / "report_star_mismatch.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["permalink","star_count","snapshot_sum"]); w.writerows(diff)

    print(f"OK: {len(all_comments)} comments, {len(all_stars)} stars → {outdir}")

if __name__ == "__main__":
    main()
