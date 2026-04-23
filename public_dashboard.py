"""
public_dashboard.py

Read-only public stats dashboard.
No management routes — stats only.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv

from database.queries import (
    get_all_channels,
    get_channel,
    get_channel_videos,
    get_global_stats,
    get_channel_rollups,
    get_recent_cron_runs,
)

load_dotenv()

app = FastAPI(docs_url=None, redoc_url=None)

ET = ZoneInfo("America/New_York")

# Channel identities not disclosed while experiment is active.
CHANNEL_CODENAMES = {
    "digital-overlords": "Project NERO",
    "villian-monologues": "Project ECHO",
}


def _codename(slug: str, fallback: str) -> str:
    return CHANNEL_CODENAMES.get(slug, fallback)


def _fmt_num(n) -> str:
    n = int(n or 0)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def _fmt_date(ts) -> str:
    if not ts:
        return "—"
    try:
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ZoneInfo("UTC"))
        return ts.astimezone(ET).strftime("%b %d, %Y")
    except Exception:
        return str(ts)[:10]


CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0a0a0a; color: #e8e8e8; min-height: 100vh; }
a { color: inherit; text-decoration: none; }
.header { border-bottom: 1px solid #1e1e1e; padding: 20px 40px; }
.header h1 { font-size: 18px; font-weight: 600; }
.header p { font-size: 13px; color: #555; margin-top: 2px; }
.container { max-width: 1100px; margin: 0 auto; padding: 40px 24px; }
.stats-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
             gap: 16px; margin-bottom: 48px; }
.stat-card { background: #111; border: 1px solid #1e1e1e; border-radius: 10px; padding: 20px; }
.stat-card .label { font-size: 11px; color: #555; text-transform: uppercase;
                    letter-spacing: 0.6px; margin-bottom: 8px; }
.stat-card .value { font-size: 28px; font-weight: 700; letter-spacing: -1px; }
.stat-card .sub { font-size: 12px; color: #444; margin-top: 4px; }
.section-title { font-size: 11px; font-weight: 600; text-transform: uppercase;
                 letter-spacing: 0.8px; color: #666; margin-bottom: 16px; }
.channels-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
                 gap: 16px; margin-bottom: 48px; }
.channel-card { background: #111; border: 1px solid #1e1e1e; border-radius: 10px;
                padding: 24px; position: relative; }
.channel-card .card-link { position: absolute; inset: 0; z-index: 0; }
.channel-card .card-content { position: relative; z-index: 1; }
.channel-card .name { font-size: 16px; font-weight: 600; margin-bottom: 4px; }
.channel-card .desc { font-size: 13px; color: #666; margin-bottom: 20px; line-height: 1.5; }
.channel-card .metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.metric .m-label { font-size: 11px; color: #555; text-transform: uppercase;
                   letter-spacing: 0.4px; margin-bottom: 4px; }
.metric .m-value { font-size: 20px; font-weight: 700; letter-spacing: -0.5px; }
.metric .m-sub { font-size: 11px; color: #444; margin-top: 2px; }
.platform-row { display: flex; gap: 8px; margin-bottom: 20px; flex-wrap: wrap; }
.badge { display: inline-flex; align-items: center; gap: 5px; font-size: 11px;
         padding: 5px 10px; border-radius: 20px; border: 1px solid #1e1e1e;
         color: #555; cursor: default; }
.badge.yt { border-color: #2e1414; color: #994433; background: rgba(255,51,88,0.05); }
.badge.tt { border-color: #14142e; color: #445599; background: rgba(107,107,255,0.05); }
.perf-table { width: 100%; border-collapse: collapse; }
.perf-table th { font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px;
                 color: #555; text-align: left; padding: 10px 12px;
                 border-bottom: 1px solid #1e1e1e; background: #0e0e0e; }
.perf-table td { font-size: 13px; padding: 10px 12px; border-bottom: 1px solid #141414;
                 color: #ccc; }
.perf-table td.label-col { color: #666; font-size: 12px; }
.perf-table tr:nth-child(even) td { background: #0d0d0d; }
.view-bar-wrap { display: flex; align-items: center; gap: 10px; }
.view-bar { height: 3px; background: #2a2a2a; border-radius: 2px; flex: 1; max-width: 80px; }
.view-bar-fill { height: 3px; background: #3a5a3a; border-radius: 2px; }
.back { font-size: 13px; color: #555; margin-bottom: 24px; display: inline-block; }
.back:hover { color: #aaa; }
.divider { border: none; border-top: 1px solid #1e1e1e; margin: 40px 0; }
.experiment-note { font-size: 12px; color: #444; margin-top: 40px; padding-top: 20px;
                   border-top: 1px solid #1a1a1a; text-align: center; }
footer { text-align: center; padding: 32px; font-size: 12px; color: #444;
         border-top: 1px solid #1a1a1a; margin-top: 40px; }
"""


def _base(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{CSS}</style>
</head>
<body>
<div class="header">
  <h1>Content Engine</h1>
  <p>Autonomous AI video publishing</p>
</div>
<div class="container">
{body}
</div>
<footer>Updated live · Powered by Claude + automated pipeline</footer>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    stats = get_global_stats()
    channels = get_all_channels()
    rollups = get_channel_rollups()
    cron_runs = get_recent_cron_runs(limit=1)
    last_run = cron_runs[0] if cron_runs else None

    total_published = int(stats.get('total_posted') or 0)
    total_views = int(stats.get('total_views') or 0)
    views_per_video = (total_views // total_published) if total_published else 0

    stats_html = f"""
<div class="stats-row">
  <div class="stat-card">
    <div class="label">Total Views</div>
    <div class="value">{_fmt_num(total_views)}</div>
    <div class="sub">YouTube</div>
  </div>
  <div class="stat-card">
    <div class="label">Videos Published</div>
    <div class="value">{_fmt_num(total_published)}</div>
    <div class="sub">across all channels</div>
  </div>
  <div class="stat-card">
    <div class="label">Views / Video</div>
    <div class="value">{_fmt_num(views_per_video)}</div>
    <div class="sub">avg per published</div>
  </div>
  <div class="stat-card">
    <div class="label">Channels</div>
    <div class="value">{stats['total_channels']}</div>
    <div class="sub">active</div>
  </div>
  <div class="stat-card">
    <div class="label">Last Sync</div>
    <div class="value" style="font-size:16px;letter-spacing:0">{_fmt_date(last_run['started_at']) if last_run else '—'}</div>
    <div class="sub">{last_run['status'] if last_run else ''}</div>
  </div>
</div>
"""

    cards = ""
    for ch in channels:
        if ch.get("status") != "active":
            continue
        slug = ch["slug"]
        r = rollups.get(slug, {})
        videos = get_channel_videos(slug)
        published = [v for v in videos if v.get('youtube_status') == 'posted']
        total_yt_views = sum(v.get("youtube_views") or 0 for v in videos)
        vpv = (total_yt_views // len(published)) if published else 0
        codename = _codename(slug, ch['name'])
        avg_pct = ch.get("avg_view_percentage")
        avg_pct_str = f"{avg_pct:.1f}%" if avg_pct else "—"

        cards += f"""
<div class="channel-card">
  <a href="/channel/{slug}" class="card-link"></a>
  <div class="card-content">
    <div class="name">{codename}</div>
    <div class="desc">{ch.get('description', '')[:100]}</div>
    <div class="metrics">
      <div class="metric">
        <div class="m-label">Views</div>
        <div class="m-value">{_fmt_num(total_yt_views)}</div>
        <div class="m-sub">{len(published)} videos</div>
      </div>
      <div class="metric">
        <div class="m-label">Views / Video</div>
        <div class="m-value">{_fmt_num(vpv)}</div>
      </div>
      <div class="metric">
        <div class="m-label">Avg Retention</div>
        <div class="m-value">{avg_pct_str}</div>
      </div>
      <div class="metric">
        <div class="m-label">Likes</div>
        <div class="m-value">{_fmt_num(r.get('youtube_likes', 0))}</div>
      </div>
    </div>
  </div>
</div>"""

    body = f"""
<div class="section-title">Overview</div>
{stats_html}
<div class="section-title">Channels</div>
<div class="channels-grid">{cards}</div>
<p class="experiment-note">Channel identities are not disclosed while the experiment is active.</p>
"""
    return HTMLResponse(_base("Content Engine", body))


@app.get("/channel/{slug}", response_class=HTMLResponse)
async def channel_page(slug: str, request: Request):
    ch = get_channel(slug)
    if not ch:
        return HTMLResponse("<h1>Channel not found</h1>", status_code=404)

    codename = _codename(slug, ch['name'])
    videos = get_channel_videos(slug)
    published = [v for v in videos if v.get('youtube_status') == 'posted']
    published_sorted = sorted(published, key=lambda v: v.get('youtube_views') or 0, reverse=True)

    total_yt = sum(v.get("youtube_views") or 0 for v in videos)
    vpv = (total_yt // len(published)) if published else 0
    max_views = max((v.get("youtube_views") or 0 for v in published_sorted), default=1) or 1

    avg_duration = ch.get("avg_view_duration_secs")
    avg_pct = ch.get("avg_view_percentage")
    avg_duration_str = f"{int(avg_duration)}s" if avg_duration else "—"
    avg_pct_str = f"{avg_pct:.1f}%" if avg_pct else "—"

    yt_badge = '<span class="badge yt">▶ YouTube</span>' if ch.get("youtube_channel_url") else ""

    rows = ""
    for i, v in enumerate(published_sorted, start=1):
        views = v.get("youtube_views") or 0
        bar_pct = int((views / max_views) * 100)
        rows += f"""<tr>
          <td class="label-col">Unit #{i:02d}</td>
          <td>{_fmt_date(v.get('youtube_posted_at') or v.get('posted_at') or v.get('scheduled_for'))}</td>
          <td>
            <div class="view-bar-wrap">
              <span>{_fmt_num(views)}</span>
              <div class="view-bar"><div class="view-bar-fill" style="width:{bar_pct}%"></div></div>
            </div>
          </td>
        </tr>"""

    body = f"""
<a href="/" class="back">← All channels</a>
<div class="section-title">Channel</div>
<div style="margin-bottom:32px">
  <h2 style="font-size:24px;font-weight:700;margin-bottom:8px">{codename}</h2>
  <p style="color:#666;font-size:14px;margin-bottom:16px">{ch.get('description','')}</p>
  <div class="platform-row">{yt_badge}</div>
</div>
<div class="stats-row">
  <div class="stat-card">
    <div class="label">Views</div>
    <div class="value">{_fmt_num(total_yt)}</div>
    <div class="sub">YouTube</div>
  </div>
  <div class="stat-card">
    <div class="label">Views / Video</div>
    <div class="value">{_fmt_num(vpv)}</div>
    <div class="sub">avg per published</div>
  </div>
  <div class="stat-card">
    <div class="label">Avg View Duration</div>
    <div class="value">{avg_duration_str}</div>
    <div class="sub">per view</div>
  </div>
  <div class="stat-card">
    <div class="label">Avg Retention</div>
    <div class="value">{avg_pct_str}</div>
    <div class="sub">of video watched</div>
  </div>
  <div class="stat-card">
    <div class="label">Videos</div>
    <div class="value">{len(published)}</div>
    <div class="sub">published</div>
  </div>
  <div class="stat-card">
    <div class="label">Live Since</div>
    <div class="value" style="font-size:16px;letter-spacing:0">{_fmt_date(ch.get('created_at'))}</div>
  </div>
</div>
<hr class="divider">
<div class="section-title">Performance by Unit</div>
<table class="perf-table">
  <thead><tr>
    <th>Unit</th><th>Published</th><th>Views</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>
<p class="experiment-note">Channel identity not disclosed while experiment is active.</p>
"""
    return HTMLResponse(_base(codename, body))
