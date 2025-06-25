#!/usr/bin/env python3
"""
Сбор geo-ID Циана для Москвы и МО с параметром --base.
"""

import aiohttp, asyncio, re, json, argparse
from pathlib import Path
from aiohttp import ClientResponseError, TCPConnector

# Пагинация
STEP_REGIONS = 1000
STEP_STREETS = 10000
DUMP_FILENAME = "moscow_mo_geo.json"

# Заголовки
HEADERS = {
    "x-cian-client-type": "web",
    "x-cian-app-version": "2.0.0",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "ru,en;q=0.9",
    "Referer": "https://www.cian.ru/"
}


async def _fetch(session, base, endpoint, **params):
    """GET с retry на 503/504."""
    url = f"{base}/{endpoint}"
    try:
        async with session.get(url, params=params, headers=HEADERS) as r:
            r.raise_for_status()
            return await r.json()
    except ClientResponseError as e:
        if e.status in (503, 504):
            await asyncio.sleep(0.5)
            return await _fetch(session, base, endpoint, **params)
        raise


async def _paginate(session, base, endpoint, step, **params):
    """Постраничная загрузка."""
    offset = 0
    while True:
        data = await _fetch(session, base, endpoint,
                            offset=offset, limit=step, **params)
        for itm in data.get("items", []):
            yield itm
        offset += step
        if offset >= data.get("total", 0):
            break


async def _get_subject_ids(session, base):
    """ID Москвы и МО (type=1)."""
    subs = [s async for s in _paginate(session, base, "regions", STEP_REGIONS, type=1)]
    msk = next(s["id"] for s in subs if s["name"] == "Москва")
    mo  = next(s["id"] for s in subs if s["name"] == "Московская область")
    return msk, mo


async def _collect_regions(session, base, parent_id, *types):
    regs = []
    for t in types:
        regs += [r async for r in _paginate(session, base, "regions", STEP_REGIONS,
                                           parentId=parent_id, type=t)]
    return regs


async def _collect_streets(session, base, region_id):
    return [s async for s in _paginate(session, base, "streets", STEP_STREETS,
                                      regionId=region_id)]


async def _download_all(session, base, dump_path: Path):
    """Скачивает районы и улицы, сохраняет JSON."""
    msk_id, mo_id = await _get_subject_ids(session, base)

    # Москва: районы (type=4)
    msk_ds = await _collect_regions(session, base, msk_id, 4)
    # МО: города и районы (type=2,3)
    mo_ps  = await _collect_regions(session, base, mo_id, 2, 3)

    all_objs = []
    for reg in msk_ds + mo_ps:
        obj_type = "район" if reg["parentId"] == msk_id else "нас.пункт"
        all_objs.append({
            "id":       reg["id"],
            "name":     reg["name"],
            "type":     obj_type,
            "parentId": reg["parentId"]
        })
        try:
            for st in await _collect_streets(session, base, reg["id"]):
                all_objs.append({
                    "id":       st["id"],
                    "name":     st["name"],
                    "type":     "улица",
                    "parentId": st["regionId"]
                })
        except Exception as e:
            print(f"⚠️  Пропущена улица для {reg['name']} ({reg['id']}): {e}")

    dump_path.write_text(json.dumps(all_objs, ensure_ascii=False, indent=2), "utf-8")
    return all_objs


def _build_index(items):
    idx = {}
    for o in items:
        idx.setdefault(o["name"].lower(), []).append(o)
    return idx


def find_location(query: str, index):
    q = query.strip().lower()
    exact = index.get(q, [])
    if exact:
        return exact
    pat = re.escape(q)
    return [o for name, lst in index.items()
            if re.search(pat, name)
            for o in lst]


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="https://geo.cian.ru/geo/v5",
                   help="URL API, например https://geo.cian.ru/geo/v5 или /v6, /v7")
    p.add_argument("--dump", default=DUMP_FILENAME)
    p.add_argument("--skip-fetch", action="store_true")
    p.add_argument("--query")
    args = p.parse_args()

    dump = Path(args.dump)
    base = args.base.rstrip("/")

    if args.skip_fetch and dump.exists():
        items = json.loads(dump.read_text("utf-8"))
    else:
        print(f"⏬  Загружаем с {base} …")
        async with aiohttp.ClientSession(connector=TCPConnector(limit_per_host=2)) as sess:
            items = await _download_all(sess, base, dump)
        print(f"✅  Сохранено {len(items):,} объектов → {dump}")

    index = _build_index(items)

    if args.query:
        res = find_location(args.query, index)
        if res:
            for o in res:
                print(f"{o['name']} ({o['type']}) → id={o['id']}")
        else:
            print("❌  Совпадений не найдено.")


if __name__ == "__main__":
    asyncio.run(main())
