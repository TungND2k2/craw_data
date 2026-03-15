"""
Wattpad Auto Pipeline Server
- POST /api/submit: nhan link Wattpad -> scrape -> enqueue
- GET /api/stories: danh sach stories + trang thai
- GET /api/stories/{id}: chi tiet 1 story
- GET /api/events: SSE realtime updates
- GET /: dashboard web
"""

import re
import os
import socket
import time
import json
import uuid
import threading
import asyncio
from datetime import datetime
from typing import Optional

import requests
import cloudscraper
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse
import uvicorn

# --- DNS patch ---
WATTPAD_REAL_IP = "52.84.150.37"
_original_getaddrinfo = socket.getaddrinfo


def _patched_getaddrinfo(host, port, *args, **kwargs):
    if host in ("www.wattpad.com", "wattpad.com"):
        return _original_getaddrinfo(WATTPAD_REAL_IP, port, *args, **kwargs)
    return _original_getaddrinfo(host, port, *args, **kwargs)


socket.getaddrinfo = _patched_getaddrinfo

# --- Config ---
TTS_BASE_URL = os.getenv("TTS_BASE_URL", "https://formed-strings-looked-grab.trycloudflare.com")
TTS_API_KEY = os.getenv("TTS_API_KEY", "bf9b410fc6c3ba2721e8151bb3d9c97616ac1003979d89fd9794add50e402f1d")
BACKEND_URL = os.getenv("BACKEND_URL", "http://103.252.73.220/api")
BACKEND_EMAIL = os.getenv("BACKEND_EMAIL", "admin@ptc.vn")
BACKEND_PASS = os.getenv("BACKEND_PASS", "admin123")

# --- In-memory store ---
# stories = {id: {id, url, title, description, author, status, chapters: [{name, id, text, tts_job_id, tts_status, audio_key, backend_id}], created_at, error}}
stories = {}
# queue of story IDs waiting to be processed
story_queue = []
queue_lock = threading.Lock()

# SSE subscribers
sse_subscribers = []


def broadcast_event(event_type: str, data: dict):
    msg = json.dumps(data, ensure_ascii=False, default=str)
    for q in list(sse_subscribers):
        try:
            q.put_nowait({"event": event_type, "data": msg})
        except Exception:
            pass


def update_story(story_id: str, **kwargs):
    if story_id in stories:
        stories[story_id].update(kwargs)
        broadcast_event("story_update", {"id": story_id, **stories[story_id]})


def update_chapter(story_id: str, ch_idx: int, **kwargs):
    if story_id in stories and ch_idx < len(stories[story_id]["chapters"]):
        stories[story_id]["chapters"][ch_idx].update(kwargs)
        broadcast_event("chapter_update", {
            "story_id": story_id,
            "chapter_idx": ch_idx,
            **stories[story_id]["chapters"][ch_idx],
        })


# ===================== WATTPAD =====================


def get_scraper():
    return cloudscraper.create_scraper()


def fetch_story_info(url: str):
    scraper = get_scraper()
    resp = scraper.get(url.strip(), timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    title_tag = soup.find("title")
    title = title_tag.text.split(" - Wattpad")[0].strip() if title_tag else "Unknown"
    desc_tag = soup.find("meta", {"name": "description"})
    description = desc_tag["content"].strip() if desc_tag else ""

    chapter_links = soup.find_all("a", href=re.compile(r"wattpad\.com/\d+"))
    chapters = []
    seen_ids = set()
    for a in chapter_links:
        href = a.get("href", "")
        m = re.search(r"/(\d+)", href)
        if m and m.group(1) not in seen_ids:
            seen_ids.add(m.group(1))
            chap_name = a.get_text(strip=True)
            chap_name = re.sub(
                r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+\w+\s+\d+,\s+\d+$", "", chap_name
            ).strip()
            if not chap_name:
                chap_name = f"Chapter {len(chapters) + 1}"
            chapters.append({"wattpad_id": m.group(1), "name": chap_name})

    return title, description, chapters


def fetch_chapter_text(part_id: str) -> str:
    scraper = get_scraper()
    api_url = f"https://www.wattpad.com/apiv2/storytext?id={part_id}"
    resp = scraper.get(api_url, timeout=20)
    if resp.status_code == 200 and resp.text.strip():
        soup = BeautifulSoup(resp.text, "html.parser")
        return soup.get_text(separator="\n").strip()
    return ""


# ===================== TTS =====================


def submit_tts(text: str, voice: str, speed: float, filename: str = "") -> str:
    body = {"input": text, "voice": voice, "speed": speed, "response_format": "mp3"}
    if filename:
        body["filename"] = filename
    resp = requests.post(
        f"{TTS_BASE_URL}/v1/audio/speech/async",
        headers={"X-API-Key": TTS_API_KEY, "Content-Type": "application/json"},
        json=body, timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["job_id"]


def poll_tts(job_id: str) -> dict:
    resp = requests.get(
        f"{TTS_BASE_URL}/v1/jobs/{job_id}",
        headers={"X-API-Key": TTS_API_KEY},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ===================== BACKEND =====================


def backend_login() -> str:
    resp = requests.post(
        f"{BACKEND_URL}/auth/login",
        json={"email": BACKEND_EMAIL, "password": BACKEND_PASS},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("accessToken", "")


def backend_create_story(token: str, title: str, description: str, author: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", title.lower())
    slug = re.sub(r"[\s]+", "-", slug).strip("-")
    resp = requests.post(
        f"{BACKEND_URL}/stories",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "title": title, "slug": slug, "description": description,
            "authorName": author, "storyStatus": "ongoing",
            "publishStatus": "published", "isFeatured": False, "tags": [],
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("id", "")


def backend_create_chapter(
    token: str, story_id: str, chapter_number: int,
    title: str, content: str, audio_key: str = "",
) -> str:
    body = {
        "storyId": story_id, "chapterNumber": chapter_number,
        "title": title, "content": content,
        "chapterType": "audio" if audio_key else "text",
        "duration": 0, "price": 0, "isFree": True, "status": "published",
    }
    if audio_key:
        body["audioKey"] = audio_key
    resp = requests.post(
        f"{BACKEND_URL}/chapters",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body, timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("id", "")


# ===================== WORKER =====================


def process_story(story_id: str):
    """Process a single story: scrape -> TTS all chapters -> push backend."""
    story = stories[story_id]
    voice = story.get("voice", "quynh")
    speed = story.get("speed", 0.95)

    try:
        # Step 1: Scrape
        update_story(story_id, status="scraping")
        title, description, raw_chapters = fetch_story_info(story["url"])
        story["title"] = title
        story["description"] = description
        story["chapters"] = [
            {
                "name": ch["name"], "wattpad_id": ch["wattpad_id"],
                "text": "", "tts_job_id": "", "tts_status": "pending",
                "audio_key": "", "backend_id": "",
            }
            for ch in raw_chapters
        ]
        update_story(story_id, status="scraping", title=title,
                     description=description, total_chapters=len(raw_chapters))

        # Step 2: Fetch text + submit TTS for each chapter
        update_story(story_id, status="tts_submitting")
        for i, ch in enumerate(story["chapters"]):
            update_chapter(story_id, i, tts_status="fetching_text")
            text = fetch_chapter_text(ch["wattpad_id"])
            if not text:
                update_chapter(story_id, i, tts_status="skipped", text="")
                continue

            update_chapter(story_id, i, text=text, tts_status="submitting")
            filename = f"Chuong {i + 1}.mp3"
            try:
                job_id = submit_tts(text, voice, speed, filename=filename)
                update_chapter(story_id, i, tts_job_id=job_id, tts_status="pending")
            except Exception as e:
                update_chapter(story_id, i, tts_status=f"error: {e}")

        # Step 3: Wait for all TTS jobs
        update_story(story_id, status="tts_processing")
        while True:
            all_done = True
            for i, ch in enumerate(story["chapters"]):
                if ch["tts_status"] in ("completed", "skipped") or ch["tts_status"].startswith("error"):
                    continue
                if not ch["tts_job_id"]:
                    continue
                all_done = False
                try:
                    data = poll_tts(ch["tts_job_id"])
                    status = data.get("status", "unknown")
                    if status == "completed":
                        audio_key = data.get("s3_url", "") or data.get("output_path", "") or ch["tts_job_id"]
                        update_chapter(story_id, i, tts_status="completed", audio_key=audio_key)
                    elif status == "failed":
                        update_chapter(story_id, i, tts_status=f"error: {data.get('error', '?')}")
                    else:
                        progress = data.get("progress", 0)
                        update_chapter(story_id, i, tts_status=f"processing ({progress}%)")
                except Exception:
                    pass

            if all_done:
                break
            time.sleep(5)

        # Step 4: Push to backend
        update_story(story_id, status="pushing")
        token = backend_login()

        author = story.get("author", "") or "Unknown"
        backend_story_id = backend_create_story(token, title, description, author)
        update_story(story_id, backend_story_id=backend_story_id)

        chap_num = 0
        for i, ch in enumerate(story["chapters"]):
            if ch["tts_status"] != "completed":
                continue
            chap_num += 1
            update_chapter(story_id, i, tts_status="pushing")
            try:
                bid = backend_create_chapter(
                    token, backend_story_id, chap_num,
                    ch["name"], ch["text"], ch["audio_key"],
                )
                update_chapter(story_id, i, backend_id=bid, tts_status="done")
            except Exception as e:
                update_chapter(story_id, i, tts_status=f"push_error: {e}")

        update_story(story_id, status="completed")

    except Exception as e:
        update_story(story_id, status="error", error=str(e))


def worker_loop():
    """Background worker: picks stories from queue and processes them one by one."""
    while True:
        story_id = None
        with queue_lock:
            if story_queue:
                story_id = story_queue.pop(0)

        if story_id:
            try:
                process_story(story_id)
            except Exception as e:
                update_story(story_id, status="error", error=str(e))
        else:
            time.sleep(1)


# Start worker thread
worker_thread = threading.Thread(target=worker_loop, daemon=True)
worker_thread.start()


# ===================== FASTAPI =====================

app = FastAPI(title="Wattpad Pipeline")


@app.post("/api/submit")
async def submit_story(request: Request):
    body = await request.json()
    url = body.get("url", "").strip()
    if not url or "wattpad.com/story/" not in url:
        return JSONResponse({"error": "Invalid Wattpad story URL"}, status_code=400)

    story_id = str(uuid.uuid4())[:8]
    stories[story_id] = {
        "id": story_id,
        "url": url,
        "title": "",
        "description": "",
        "author": body.get("author", ""),
        "voice": body.get("voice", "quynh"),
        "speed": body.get("speed", 0.95),
        "status": "queued",
        "chapters": [],
        "total_chapters": 0,
        "backend_story_id": "",
        "error": "",
        "created_at": datetime.now().isoformat(),
    }

    with queue_lock:
        story_queue.append(story_id)

    broadcast_event("story_added", stories[story_id])
    return {"id": story_id, "status": "queued", "position": len(story_queue)}


@app.get("/api/stories")
async def list_stories():
    result = []
    for sid in reversed(list(stories.keys())):
        s = stories[sid]
        done = sum(1 for ch in s["chapters"] if ch.get("tts_status") == "done")
        total = len(s["chapters"])
        result.append({
            "id": s["id"], "title": s["title"] or s["url"],
            "status": s["status"], "done": done, "total": total,
            "created_at": s["created_at"],
        })
    return result


@app.get("/api/stories/{story_id}")
async def get_story(story_id: str):
    if story_id not in stories:
        return JSONResponse({"error": "Not found"}, status_code=404)
    return stories[story_id]


@app.get("/api/queue")
async def get_queue():
    with queue_lock:
        return {"queue": list(story_queue), "length": len(story_queue)}


@app.get("/api/events")
async def sse_events(request: Request):
    import asyncio as aio
    q = aio.Queue()
    sse_subscribers.append(q)

    async def event_generator():
        try:
            # Send current state
            yield {"event": "init", "data": json.dumps(list(stories.values()), ensure_ascii=False, default=str)}
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await aio.wait_for(q.get(), timeout=30)
                    yield msg
                except aio.TimeoutError:
                    yield {"event": "ping", "data": ""}
        finally:
            sse_subscribers.remove(q)

    return EventSourceResponse(event_generator())


# ===================== DASHBOARD =====================

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Wattpad Pipeline</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }

.header { background: linear-gradient(135deg, #1e293b, #334155); padding: 20px 30px; border-bottom: 1px solid #475569; }
.header h1 { font-size: 1.5rem; color: #f1f5f9; }
.header p { color: #94a3b8; font-size: 0.85rem; margin-top: 4px; }

.container { max-width: 1200px; margin: 0 auto; padding: 20px; }

/* Submit form */
.submit-card {
    background: #1e293b; border: 1px solid #334155; border-radius: 12px;
    padding: 24px; margin-bottom: 24px;
}
.submit-card h2 { font-size: 1.1rem; margin-bottom: 16px; color: #f1f5f9; }
.form-row { display: flex; gap: 12px; margin-bottom: 12px; }
.form-row input, .form-row select {
    flex: 1; padding: 10px 14px; border-radius: 8px; border: 1px solid #475569;
    background: #0f172a; color: #e2e8f0; font-size: 0.9rem; outline: none;
}
.form-row input:focus { border-color: #3b82f6; }
.form-row input.url-input { flex: 4; }
.btn {
    padding: 10px 24px; border-radius: 8px; border: none; cursor: pointer;
    font-weight: 600; font-size: 0.9rem; transition: all 0.2s;
}
.btn-primary { background: #3b82f6; color: white; }
.btn-primary:hover { background: #2563eb; }
.btn-primary:disabled { background: #475569; cursor: not-allowed; }

/* Queue & Stories */
.section-title { font-size: 1rem; color: #94a3b8; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
.section-title .badge {
    background: #3b82f6; color: white; padding: 2px 8px; border-radius: 10px; font-size: 0.75rem;
}

.story-card {
    background: #1e293b; border: 1px solid #334155; border-radius: 10px;
    margin-bottom: 12px; overflow: hidden; transition: border-color 0.2s;
}
.story-card:hover { border-color: #475569; }
.story-card.processing { border-color: #3b82f6; }
.story-card.completed { border-color: #22c55e; }
.story-card.error { border-color: #ef4444; }

.story-header {
    padding: 16px 20px; cursor: pointer; display: flex; justify-content: space-between; align-items: center;
}
.story-header h3 { font-size: 0.95rem; color: #f1f5f9; }
.story-meta { display: flex; gap: 12px; align-items: center; }

.status-badge {
    padding: 4px 10px; border-radius: 6px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase;
}
.status-queued { background: #475569; color: #e2e8f0; }
.status-scraping, .status-tts_submitting { background: #f59e0b22; color: #f59e0b; border: 1px solid #f59e0b44; }
.status-tts_processing { background: #3b82f622; color: #3b82f6; border: 1px solid #3b82f644; }
.status-pushing { background: #8b5cf622; color: #8b5cf6; border: 1px solid #8b5cf644; }
.status-completed { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e44; }
.status-error { background: #ef444422; color: #ef4444; border: 1px solid #ef444444; }

.progress-bar-bg {
    height: 3px; background: #334155; width: 100%;
}
.progress-bar {
    height: 100%; background: linear-gradient(90deg, #3b82f6, #22c55e); transition: width 0.5s;
}

.story-detail { display: none; padding: 0 20px 16px; }
.story-detail.open { display: block; }

.chapter-list { list-style: none; }
.chapter-item {
    display: flex; justify-content: space-between; align-items: center;
    padding: 8px 12px; border-radius: 6px; margin-bottom: 4px; font-size: 0.85rem;
    background: #0f172a;
}
.chapter-item .ch-name { color: #cbd5e1; flex: 1; }
.chapter-item .ch-status { font-size: 0.75rem; }

.ch-pending { color: #64748b; }
.ch-fetching_text, .ch-submitting { color: #f59e0b; }
.ch-processing { color: #3b82f6; }
.ch-completed { color: #22c55e; }
.ch-pushing { color: #8b5cf6; }
.ch-done { color: #22c55e; font-weight: 600; }
.ch-skipped { color: #64748b; }
.ch-error { color: #ef4444; }

.empty-state { text-align: center; color: #64748b; padding: 40px; }

/* Connection indicator */
.conn-status { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.conn-status.connected { background: #22c55e; }
.conn-status.disconnected { background: #ef4444; }
</style>
</head>
<body>

<div class="header">
    <h1>Wattpad Auto Pipeline</h1>
    <p><span class="conn-status disconnected" id="connDot"></span><span id="connText">Dang ket noi...</span></p>
</div>

<div class="container">
    <div class="submit-card">
        <h2>Them truyen moi</h2>
        <div class="form-row">
            <input type="text" class="url-input" id="urlInput" placeholder="https://www.wattpad.com/story/358766695-...">
            <input type="text" id="authorInput" placeholder="Tac gia (tuy chon)" style="flex:1">
        </div>
        <div class="form-row">
            <select id="voiceSelect">
                <option value="quynh">Quynh</option>
                <option value="lien">Lien</option>
                <option value="hung">Hung</option>
                <option value="tuan">Tuan</option>
            </select>
            <select id="speedSelect">
                <option value="0.9">0.9x</option>
                <option value="0.95" selected>0.95x</option>
                <option value="1.0">1.0x</option>
                <option value="1.1">1.1x</option>
            </select>
            <button class="btn btn-primary" id="submitBtn" onclick="submitStory()">Them vao hang doi</button>
        </div>
    </div>

    <div class="section-title">Hang doi & Truyen <span class="badge" id="totalCount">0</span></div>
    <div id="storiesList">
        <div class="empty-state">Chua co truyen nao. Dan link Wattpad phia tren de bat dau.</div>
    </div>
</div>

<script>
let storiesData = {};

function submitStory() {
    const url = document.getElementById('urlInput').value.trim();
    if (!url) return;
    const btn = document.getElementById('submitBtn');
    btn.disabled = true;
    btn.textContent = 'Dang gui...';

    fetch('/api/submit', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            url: url,
            author: document.getElementById('authorInput').value,
            voice: document.getElementById('voiceSelect').value,
            speed: parseFloat(document.getElementById('speedSelect').value),
        })
    })
    .then(r => r.json())
    .then(data => {
        if (data.error) { alert(data.error); }
        else { document.getElementById('urlInput').value = ''; }
    })
    .catch(e => alert('Loi: ' + e))
    .finally(() => { btn.disabled = false; btn.textContent = 'Them vao hang doi'; });
}

function getStatusClass(status) {
    if (!status) return 'status-queued';
    const s = status.split(':')[0].replace(/ /g, '');
    const map = {
        'queued': 'status-queued', 'scraping': 'status-scraping',
        'tts_submitting': 'status-tts_submitting', 'tts_processing': 'status-tts_processing',
        'pushing': 'status-pushing', 'completed': 'status-completed', 'error': 'status-error',
    };
    return map[s] || 'status-queued';
}

function getChapterStatusClass(status) {
    if (!status) return 'ch-pending';
    if (status.startsWith('error') || status.startsWith('push_error')) return 'ch-error';
    if (status.startsWith('processing')) return 'ch-processing';
    const map = {
        'pending': 'ch-pending', 'fetching_text': 'ch-fetching_text',
        'submitting': 'ch-submitting', 'completed': 'ch-completed',
        'pushing': 'ch-pushing', 'done': 'ch-done', 'skipped': 'ch-skipped',
    };
    return map[status] || 'ch-pending';
}

function getStatusLabel(status) {
    const labels = {
        'queued': 'Cho', 'scraping': 'Dang scrape', 'tts_submitting': 'Gui TTS',
        'tts_processing': 'Dang TTS', 'pushing': 'Day backend', 'completed': 'Xong', 'error': 'Loi',
    };
    return labels[status] || status;
}

function getProgress(story) {
    if (!story.chapters || story.chapters.length === 0) return 0;
    const done = story.chapters.filter(c => c.tts_status === 'done' || c.tts_status === 'completed').length;
    return Math.round((done / story.chapters.length) * 100);
}

function toggleDetail(storyId) {
    const el = document.getElementById('detail-' + storyId);
    if (el) el.classList.toggle('open');
}

function renderStories() {
    const list = document.getElementById('storiesList');
    const ids = Object.keys(storiesData).sort((a, b) => {
        return (storiesData[b].created_at || '').localeCompare(storiesData[a].created_at || '');
    });
    document.getElementById('totalCount').textContent = ids.length;

    if (ids.length === 0) {
        list.innerHTML = '<div class="empty-state">Chua co truyen nao.</div>';
        return;
    }

    let html = '';
    for (const id of ids) {
        const s = storiesData[id];
        const progress = getProgress(s);
        const doneCount = s.chapters ? s.chapters.filter(c => c.tts_status === 'done').length : 0;
        const totalCount = s.chapters ? s.chapters.length : 0;
        const statusClass = s.status === 'completed' ? 'completed' :
                           s.status === 'error' ? 'error' :
                           ['scraping','tts_submitting','tts_processing','pushing'].includes(s.status) ? 'processing' : '';

        html += '<div class="story-card ' + statusClass + '">';
        html += '<div class="story-header" onclick="toggleDetail(\\''+id+'\\')">';
        html += '<h3>' + (s.title || s.url || 'Dang tai...') + '</h3>';
        html += '<div class="story-meta">';
        html += '<span style="font-size:0.8rem;color:#64748b">' + doneCount + '/' + totalCount + '</span>';
        html += '<span class="status-badge ' + getStatusClass(s.status) + '">' + getStatusLabel(s.status) + '</span>';
        html += '</div></div>';
        html += '<div class="progress-bar-bg"><div class="progress-bar" style="width:' + progress + '%"></div></div>';

        html += '<div class="story-detail" id="detail-' + id + '">';
        if (s.error) html += '<p style="color:#ef4444;margin-bottom:8px;font-size:0.85rem">Loi: ' + s.error + '</p>';
        if (s.chapters && s.chapters.length > 0) {
            html += '<ul class="chapter-list">';
            for (let i = 0; i < s.chapters.length; i++) {
                const ch = s.chapters[i];
                const chStatusClass = getChapterStatusClass(ch.tts_status);
                html += '<li class="chapter-item">';
                html += '<span class="ch-name">' + (i+1) + '. ' + (ch.name || '') + '</span>';
                html += '<span class="ch-status ' + chStatusClass + '">' + (ch.tts_status || 'pending') + '</span>';
                html += '</li>';
            }
            html += '</ul>';
        }
        html += '</div></div>';
    }
    list.innerHTML = html;
}

// SSE connection
function connectSSE() {
    const dot = document.getElementById('connDot');
    const text = document.getElementById('connText');
    const es = new EventSource('/api/events');

    es.onopen = () => { dot.className = 'conn-status connected'; text.textContent = 'Da ket noi'; };
    es.onerror = () => { dot.className = 'conn-status disconnected'; text.textContent = 'Mat ket noi, dang thu lai...'; };

    es.addEventListener('init', (e) => {
        const data = JSON.parse(e.data);
        storiesData = {};
        for (const s of data) { storiesData[s.id] = s; }
        renderStories();
    });

    es.addEventListener('story_added', (e) => {
        const data = JSON.parse(e.data);
        storiesData[data.id] = data;
        renderStories();
    });

    es.addEventListener('story_update', (e) => {
        const data = JSON.parse(e.data);
        if (storiesData[data.id]) {
            Object.assign(storiesData[data.id], data);
        } else {
            storiesData[data.id] = data;
        }
        renderStories();
    });

    es.addEventListener('chapter_update', (e) => {
        const data = JSON.parse(e.data);
        const story = storiesData[data.story_id];
        if (story && story.chapters && story.chapters[data.chapter_idx]) {
            Object.assign(story.chapters[data.chapter_idx], data);
            renderStories();
        }
    });
}

connectSSE();

// Enter key to submit
document.getElementById('urlInput').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') submitStory();
});
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
