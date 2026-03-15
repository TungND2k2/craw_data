import re
import os
import socket
import time
import requests
import cloudscraper
from bs4 import BeautifulSoup
import gradio as gr

# --- DNS patch: bypass local redirect of wattpad.com to 127.0.0.1 ---
WATTPAD_REAL_IP = "52.84.150.37"
_original_getaddrinfo = socket.getaddrinfo


def _patched_getaddrinfo(host, port, *args, **kwargs):
    if host in ("www.wattpad.com", "wattpad.com"):
        return _original_getaddrinfo(WATTPAD_REAL_IP, port, *args, **kwargs)
    return _original_getaddrinfo(host, port, *args, **kwargs)


socket.getaddrinfo = _patched_getaddrinfo

# --- Config ---
TTS_BASE_URL = "https://formed-strings-looked-grab.trycloudflare.com"
TTS_API_KEY = "bf9b410fc6c3ba2721e8151bb3d9c97616ac1003979d89fd9794add50e402f1d"
BACKEND_URL = "http://103.252.73.220/api"
BACKEND_EMAIL = "admin@ptc.vn"
BACKEND_PASS = "admin123"


def get_scraper():
    return cloudscraper.create_scraper()


def log(lines, msg):
    lines.append(msg)
    print(msg)
    return "\n".join(lines)


# ===================== WATTPAD =====================


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
            chapters.append({"id": m.group(1), "name": chap_name, "url": href})

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


def submit_tts_job(text: str, voice: str = "quynh", speed: float = 0.95, filename: str = "") -> str:
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


def get_tts_job_status(job_id: str) -> dict:
    resp = requests.get(
        f"{TTS_BASE_URL}/v1/jobs/{job_id}",
        headers={"X-API-Key": TTS_API_KEY},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def wait_all_tts_jobs(jobs: dict, timeout: int = 1800) -> dict:
    """Poll until all jobs are completed or failed."""
    start = time.time()
    while time.time() - start < timeout:
        all_done = True
        for idx, job in jobs.items():
            if job["status"] in ("completed", "failed"):
                continue
            all_done = False
            try:
                data = get_tts_job_status(job["job_id"])
                job["status"] = data.get("status", "unknown")
                if job["status"] == "completed":
                    job["audio_key"] = data.get("s3_url", "") or data.get("output_path", "") or job["job_id"]
            except Exception:
                pass
        if all_done:
            return jobs
        time.sleep(5)
    return jobs


# ===================== BACKEND API =====================


def backend_login(api_url: str, email: str, password: str) -> str:
    resp = requests.post(f"{api_url}/auth/login", json={"email": email, "password": password}, timeout=15)
    resp.raise_for_status()
    return resp.json().get("accessToken", "")


def backend_create_story(api_url: str, token: str, title: str, description: str, author: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", title.lower())
    slug = re.sub(r"[\s]+", "-", slug).strip("-")
    resp = requests.post(
        f"{api_url}/stories",
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
    api_url: str, token: str, story_id: str,
    chapter_number: int, title: str, content: str,
    audio_key: str = "", duration: int = 0,
) -> str:
    body = {
        "storyId": story_id, "chapterNumber": chapter_number,
        "title": title, "content": content,
        "chapterType": "audio" if audio_key else "text",
        "duration": duration, "price": 0, "isFree": True, "status": "published",
    }
    if audio_key:
        body["audioKey"] = audio_key
    resp = requests.post(
        f"{api_url}/chapters",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body, timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("id", "")


# ===================== AUTO PIPELINE =====================


def run_auto(wattpad_url, voice, speed, backend_url, email, password, author_name, progress=gr.Progress()):
    """
    1 nut bam: Scrape Wattpad -> TTS tat ca chuong -> cho xong -> day len backend.
    """
    lines = []

    if not wattpad_url or "wattpad.com/story/" not in wattpad_url:
        return "Nhap link Wattpad story (dang /story/...)"

    # === STEP 1: Scrape Wattpad ===
    yield log(lines, "[1/5] Dang scrape Wattpad...")
    try:
        title, description, chapters = fetch_story_info(wattpad_url)
        yield log(lines, f"  Truyen: {title}")
        yield log(lines, f"  So chuong: {len(chapters)}")
    except Exception as e:
        yield log(lines, f"  LOI scrape: {e}")
        return

    if not chapters:
        yield log(lines, "  Khong tim thay chuong nao.")
        return

    # === STEP 2: Fetch text + submit TTS for each chapter ===
    yield log(lines, f"\n[2/5] Lay text va gui TTS ({len(chapters)} chuong)...")
    jobs = {}
    for i, ch in enumerate(chapters):
        progress((i, len(chapters)), desc=f"TTS: {ch['name']}")
        text = fetch_chapter_text(ch["id"])
        if not text:
            yield log(lines, f"  [SKIP] {ch['name']}: khong co text")
            continue
        try:
            filename = f"Chuong {i + 1}.mp3"
            job_id = submit_tts_job(text, voice=voice, speed=speed, filename=filename)
            jobs[i] = {
                "name": ch["name"], "text": text,
                "job_id": job_id, "status": "pending", "audio_key": "",
            }
            yield log(lines, f"  [SENT] {ch['name']} -> {job_id}")
        except Exception as e:
            yield log(lines, f"  [ERROR] {ch['name']}: {e}")

    if not jobs:
        yield log(lines, "  Khong gui duoc job nao.")
        return

    # === STEP 3: Wait for ALL TTS jobs (no timeout) ===
    yield log(lines, f"\n[3/5] Doi {len(jobs)} jobs hoan thanh (khong gioi han thoi gian)...")
    while True:
        done = 0
        failed = 0
        for idx, job in jobs.items():
            if job["status"] == "completed":
                done += 1
                continue
            if job["status"] == "failed":
                failed += 1
                continue
            try:
                data = get_tts_job_status(job["job_id"])
                job["status"] = data.get("status", "unknown")
                if job["status"] == "completed":
                    job["audio_key"] = data.get("s3_url", "") or data.get("output_path", "") or job["job_id"]
                    done += 1
                    yield log(lines, f"  [DONE] {job['name']} ({done}/{len(jobs)})")
                elif job["status"] == "failed":
                    failed += 1
                    yield log(lines, f"  [FAIL] {job['name']}: {data.get('error', '?')}")
            except Exception:
                pass

        progress((done + failed, len(jobs)), desc=f"TTS: {done}/{len(jobs)} xong")

        if done + failed >= len(jobs):
            break
        time.sleep(5)

    yield log(lines, f"  Ket qua: {done} thanh cong, {failed} loi")

    # === STEP 4: Login backend ===
    yield log(lines, f"\n[4/5] Dang nhap backend...")
    try:
        token = backend_login(backend_url.rstrip("/"), email, password)
        yield log(lines, f"  Dang nhap OK")
    except Exception as e:
        yield log(lines, f"  LOI dang nhap: {e}")
        return

    # === STEP 5: Create story + chapters on backend ===
    yield log(lines, f"\n[5/5] Tao truyen va day chuong len backend...")
    api = backend_url.rstrip("/")
    author = author_name.strip() or "Unknown"

    try:
        story_id = backend_create_story(api, token, title, description, author)
        yield log(lines, f"  Tao truyen OK -> ID: {story_id}")
    except Exception as e:
        yield log(lines, f"  LOI tao truyen: {e}")
        return

    success = 0
    for i, idx in enumerate(sorted(jobs.keys())):
        job = jobs[idx]
        if job["status"] != "completed":
            continue
        progress((i, len(jobs)), desc=f"Push: {job['name']}")
        try:
            chap_id = backend_create_chapter(
                api, token, story_id,
                chapter_number=i + 1,
                title=job["name"],
                content=job["text"],
                audio_key=job.get("audio_key", ""),
            )
            success += 1
            yield log(lines, f"  [OK] {job['name']} -> {chap_id}")
        except Exception as e:
            yield log(lines, f"  [FAIL] {job['name']}: {e}")

    yield log(lines, f"\n=== HOAN THANH: {success}/{len(jobs)} chuong da day len backend ===")


# ===================== GRADIO UI =====================

with gr.Blocks(title="Wattpad Auto Pipeline") as demo:
    gr.Markdown("# Wattpad Auto Pipeline\nDan link -> Nhan nut -> Tu dong: Scrape -> TTS -> Day backend")

    with gr.Row():
        url_input = gr.Textbox(
            label="Link truyen Wattpad",
            placeholder="https://www.wattpad.com/story/358766695-...",
            scale=4,
        )

    with gr.Row():
        voice_select = gr.Dropdown(
            label="Giong doc", choices=["quynh", "lien", "hung", "tuan"],
            value="quynh", scale=1,
        )
        speed_slider = gr.Slider(
            label="Toc do", minimum=0.5, maximum=2.0, value=0.95, step=0.05, scale=1,
        )
        author_input = gr.Textbox(label="Tac gia", value="", placeholder="De trong = Unknown", scale=1)

    with gr.Accordion("Cai dat Backend / TTS", open=False):
        with gr.Row():
            backend_url = gr.Textbox(label="Backend URL", value=BACKEND_URL, scale=2)
            backend_email = gr.Textbox(label="Email", value=BACKEND_EMAIL, scale=1)
            backend_pass = gr.Textbox(label="Password", value=BACKEND_PASS, type="password", scale=1)

    run_btn = gr.Button("CHAY TU DONG", variant="primary", size="lg")
    output = gr.Textbox(label="Log", lines=25, interactive=False)

    run_btn.click(
        fn=run_auto,
        inputs=[url_input, voice_select, speed_slider, backend_url, backend_email, backend_pass, author_input],
        outputs=output,
    )

if __name__ == "__main__":
    demo.launch()
