#!/usr/bin/env python3
"""Corrected wrapper for the temporary PortWatch collector.

Fixes ArcGIS's 1,000-record service cap and constructs dates from the separate
year/month/day fields used by the current Daily Ports layer.
"""
from __future__ import annotations

import time
import pandas as pd
import collect_portwatch_delivery as c


def aq_fixed(s, url, where, fields, order=None, page=1000):
    total = int(c.jget(s, url, {"where": where, "returnCountOnly": "true", "f": "json"}).get("count", 0))
    out = []
    print({"records": total, "where": where[:70]}, flush=True)
    for off in range(0, total, page):
        params = {
            "where": where,
            "outFields": fields,
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": off,
            "resultRecordCount": page,
        }
        if order:
            params["orderByFields"] = order
        features = c.jget(s, url, params).get("features", [])
        if not features:
            break
        out.append(pd.DataFrame([z.get("attributes", {}) for z in features]))
        print({"offset": off, "received": len(features)}, flush=True)
        time.sleep(0.1)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def daily_collect_fixed(s, ports):
    out = []
    ids = ports.portid.astype(str).tolist()
    for i in range(0, len(ids), 12):
        group = [z.replace("'", "''") for z in ids[i:i + 12]]
        where = " OR ".join([f"portid='{z}'" for z in group])
        print({"batch": i // 12 + 1, "ports": len(group)}, flush=True)
        query = aq_fixed(s, c.DAILY, where, c.FIELDS, "date ASC,portid ASC")
        if len(query):
            out.append(query)
    if not out:
        raise RuntimeError("No daily PortWatch data")
    x = pd.concat(out, ignore_index=True)
    x.portid = x.portid.astype(str)
    x["date"] = pd.to_datetime(
        {
            "year": pd.to_numeric(x["year"], errors="coerce"),
            "month": pd.to_numeric(x["month"], errors="coerce"),
            "day": pd.to_numeric(x["day"], errors="coerce"),
        },
        errors="coerce",
    )
    for column in [
        "portcalls_tanker", "portcalls_cargo", "portcalls", "import_tanker",
        "export_tanker", "import_cargo", "export_cargo", "import", "export",
    ]:
        x[column] = pd.to_numeric(x[column], errors="coerce").fillna(0)
    keep = [
        "portid", "lat", "lon", "LOCODE", "route_class", "hormuz_exposed_port",
        "hormuz_bypass_port", "saudi_gulf_port", "saudi_redsea_port",
        "uae_gulf_port", "uae_fujairah_port",
    ]
    x = x.merge(ports[keep], on="portid", how="left", validate="many_to_one")
    x["week"] = x.date.dt.to_period("W-MON").dt.start_time
    x["month_date"] = x.date.dt.to_period("M").dt.start_time
    return x.sort_values(["portid", "date"])


c.aq = aq_fixed
c.daily_collect = daily_collect_fixed
c.main()
