#!/usr/bin/env python3
"""
TikTok carousel scraper using TikTokApi.
Extracts photo/carousel posts (captions, music, images) from profiles.
"""
import asyncio
import csv
import json
import os
import random
import ssl
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from TikTokApi import TikTokApi
from TikTokApi.exceptions import EmptyResponseException

load_dotenv(Path(__file__).parent / ".env")

# Config
SCRIPT_DIR = Path(__file__).parent
INPUT_FILE = SCRIPT_DIR / "input.json"
INPUT_RECIPES_FILE = SCRIPT_DIR / "input_recipes.json"
LOG_FILE = SCRIPT_DIR / "scraper.log"

# Mode: "recipes" if --mode recipes or --recipes, else "nutrition"
MODE = "recipes" if ("--mode" in sys.argv and "recipes" in sys.argv) or "--recipes" in sys.argv else "nutrition"


def _log(msg: str):
    """Append to scraper.log for debugging when run from server."""
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")
    except Exception:
        pass


OUTPUT_FILE = SCRIPT_DIR / "results_carousels.json"
SEEN_IDS_FILE = SCRIPT_DIR / "scraped_ids.csv"
IMAGES_DIR = SCRIPT_DIR / "images"
LOCK_FILE = SCRIPT_DIR / "scraper.lock"
QUEUE_DIR = SCRIPT_DIR / "queue"


def load_seen_ids() -> set:
    """Load IDs already scraped from CSV."""
    seen = set()
    if SEEN_IDS_FILE.exists():
        with open(SEEN_IDS_FILE, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if row and (vid := row[0].strip()) and vid != "id":
                    seen.add(vid)
    return seen


def append_seen_ids(ids: list[str]):
    """Append new IDs to CSV."""
    with open(SEEN_IDS_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for vid in ids:
            w.writerow([vid])


def _is_locked() -> bool:
    """True if lock exists and the PID inside is a running process."""
    if not LOCK_FILE.exists():
        return False
    try:
        pid = int(LOCK_FILE.read_text().strip())
        os.kill(pid, 0)  # Check if process exists (raises if not)
        return True
    except (ValueError, OSError):
        return False


def _acquire_lock() -> bool:
    """Create lock file with current PID. Returns True if acquired."""
    LOCK_FILE.write_text(str(os.getpid()))
    return True


def _release_lock():
    """Remove lock file."""
    LOCK_FILE.unlink(missing_ok=True)


def _enqueue(mode: str) -> Path:
    """Save current carousel + images to queue. Returns path to queued item."""
    import shutil
    QUEUE_DIR.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    item_dir = QUEUE_DIR / f"pending_{ts}_{mode}"
    item_dir.mkdir()
    if OUTPUT_FILE.exists():
        shutil.copy2(OUTPUT_FILE, item_dir / "results_carousels.json")
    if IMAGES_DIR.exists():
        shutil.copytree(IMAGES_DIR, item_dir / "images")
    (item_dir / "mode.txt").write_text(mode)
    return item_dir


def _dequeue() -> Optional[tuple[Path, str]]:
    """Get next item from queue. Returns (item_dir, mode) or None."""
    if not QUEUE_DIR.exists():
        return None
    pending = sorted(QUEUE_DIR.glob("pending_*"))
    if not pending:
        return None
    item_dir = pending[0]
    mode_file = item_dir / "mode.txt"
    mode = mode_file.read_text().strip() if mode_file.exists() else "nutrition"
    return (item_dir, mode)


def _remove_from_queue(item_dir: Path):
    """Remove processed item from queue."""
    import shutil
    shutil.rmtree(item_dir, ignore_errors=True)


def load_config():
    """Load config from input.json (nutrition) or input_recipes.json (recipes mode)."""
    path = INPUT_RECIPES_FILE if MODE == "recipes" else INPUT_FILE
    if MODE == "recipes" and not path.exists():
        print(f"  Warning: {path.name} not found, falling back to {INPUT_FILE.name}", file=sys.stderr)
        path = INPUT_FILE
    with open(path) as f:
        return json.load(f)


def download_images(carousel: dict) -> dict:
    """Download photos to images/ (flat). Clears previous images first."""
    import ssl
    import shutil
    import urllib.request
    import certifi
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    photos = carousel.get("photos", [])
    if IMAGES_DIR.exists():
        shutil.rmtree(IMAGES_DIR)
    if not photos:
        return carousel
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    for i, photo in enumerate(photos):
        url = photo.get("url")
        if not url:
            continue
        ext = ".jpeg" if "jpeg" in url.lower() or "jpg" in url.lower() else ".png"
        path = IMAGES_DIR / f"{i + 1}{ext}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
                path.write_bytes(r.read())
            photo["path"] = f"images/{path.name}"
        except Exception as e:
            print(f"    Warning: could not download image {i + 1}: {e}")
    return carousel


def is_carousel(video=None, data=None) -> bool:
    """Check if post is a carousel/photo post (not video)."""
    data = data if data is not None else (getattr(video, "as_dict", None) if video else {}) or {}
    # aweme_type 68 = photo post
    if data.get("aweme_type") == 68 or data.get("awemeType") == 68:
        return True
    if data.get("imagePost") or data.get("image_post"):
        return True
    # Exclude videos (has video with duration)
    video_obj = data.get("video") or data.get("Video") or {}
    if video_obj.get("duration") or video_obj.get("duration_ms"):
        return False
    return bool(data.get("imagePost") or data.get("image_post"))


def _extract_author(author) -> dict:
    """Extract author info from TikTok data."""
    if not author or not isinstance(author, dict):
        return {}
    return {
        "username": author.get("uniqueId") or author.get("unique_id"),
        "nickname": author.get("nickname"),
        "avatar": author.get("avatarThumb") or author.get("avatar_thumb") or author.get("avatarLarger") or author.get("avatar_larger"),
        "signature": author.get("signature"),
    }


def extract_carousel(video, full_data=None) -> dict:
    """Extract caption, music, photos, author from carousel post."""
    data = full_data or getattr(video, "as_dict", None) or {}
    vid = data.get("id") or data.get("aweme_id") or data.get("awemeId") or ""

    caption = data.get("desc") or data.get("caption") or ""

    music = data.get("music") or data.get("Music") or {}
    play_url = music.get("playUrl") or music.get("play_url")
    if isinstance(play_url, dict):
        play_url = play_url.get("uri") or play_url.get("url")
    song = {
        "id": music.get("id"),
        "title": music.get("title") or music.get("Title"),
        "authorName": music.get("authorName") or music.get("author_name") or music.get("author"),
        "playUrl": play_url,
    }

    photos = []
    img_post = data.get("imagePost") or data.get("image_post") or {}
    imgs = img_post.get("images") or img_post.get("imageList") or img_post.get("image_list") or []
    for img in imgs or []:
        url_list = (
            img.get("imageURL", {}).get("urlList")
            or img.get("image_url", {}).get("url_list")
            or img.get("displayImage", {}).get("urlList")
            or img.get("display_image", {}).get("url_list")
            or img.get("urlList")
            or img.get("url_list")
            or ([img.get("url")] if img.get("url") else [])
        )
        url = url_list[0] if url_list else img.get("url")
        if url:
            photos.append({"url": url})

    author = _extract_author(data.get("author"))

    return {"id": vid, "caption": caption, "song": song, "photos": photos, "author": author}


def _parse_tiktok_url(url: str) -> tuple[Optional[str], Optional[str]]:
    """Extract username and video/photo ID from TikTok URL. Returns (username, vid) or (None, None)."""
    import re
    m = re.search(r"tiktok\.com/@([^/]+)/(?:video|photo)/(\d+)", url)
    return (m.group(1), m.group(2)) if m else (None, None)


async def fetch_single_url(url: str, mode: str = "nutrition"):
    """Fetch a single carousel from a TikTok URL."""
    username, target_id = _parse_tiktok_url(url)
    if not username or not target_id:
        print("  Invalid TikTok URL. Expected: https://www.tiktok.com/@user/video/ID or .../photo/ID", file=sys.stderr)
        return
    ms_token = os.environ.get("ms_token")
    headless = os.environ.get("TIKTOK_HEADLESS", "false").lower() == "true"
    # video.info() fails for photo posts; use user.videos() to find by ID
    video = None
    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token] if ms_token else [],
            num_sessions=1,
            sleep_after=3,
            headless=headless,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
        )
        try:
            async for v in api.user(username=username).videos(count=100):
                data = getattr(v, "as_dict", None) or {}
                vid = data.get("id") or getattr(v, "id", None) or ""
                if str(vid) == str(target_id):
                    video = v
                    break
        except Exception as e:
            print(f"  Error fetching: {e}", file=sys.stderr)
            return
    if not video:
        print(f"  Post {target_id} not found in @{username}'s recent videos.", file=sys.stderr)
        return
    data = getattr(video, "as_dict", None) or {}
    if not is_carousel(data=data):
        print("  Not a carousel/photo post.", file=sys.stderr)
        return
    carousel = extract_carousel(video, full_data=data)
    carousel = download_images(carousel)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(carousel, f, indent=2)
    print(f"  Carousel: {carousel['id']} from {url}")
    _log(f"Carousel {carousel['id']} scraped (--url mode)")
    if mode == "recipes":
        _process_with_lock("recipes")
    elif _post_to_webhook("influencer"):
        print("  Posted to influencer webhook.")
    else:
        path = _save_to_failed_queue()
        _log(f"Failed to post, saved to queue/{path.name}")
        print(f"  Failed to post, saved to {path} for retry")


async def main():
    config = load_config()
    profiles = config.get("profiles") or config.get("startUrls") or []
    if profiles and isinstance(profiles[0], str) and "tiktok.com" in profiles[0]:
        profiles = [u.split("@")[-1].split("/")[0].split("?")[0] for u in profiles]
    hashtags = [h.lstrip("#") for h in (config.get("hashtags") or [])]
    sources = [("profile", p) for p in profiles] + [("hashtag", h) for h in hashtags]
    random.shuffle(sources)
    max_items = config.get("maxItems", 25)
    if "--max" in sys.argv:
        i = sys.argv.index("--max")
        if i + 1 < len(sys.argv):
            max_items = int(sys.argv[i + 1])
    fetch_full_info = config.get("fetchFullInfo", True)

    ms_token = os.environ.get("ms_token")  # optional: from tiktok.com cookies for fewer blocks
    headless = os.environ.get("TIKTOK_HEADLESS", "false").lower() == "true"
    loop_until_feasible = "--loop-until-feasible" in sys.argv or MODE == "recipes"
    workspace = _get_workspace()
    max_attempts = 50 if loop_until_feasible else max_items

    seen_ids = load_seen_ids()
    results = []
    skipped = 0
    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token] if ms_token else [],
            num_sessions=1,
            sleep_after=3,
            headless=headless,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
        )

        for source_type, source_name in sources:
            if len(results) >= max_attempts:
                break
            src_label = f"#{source_name}" if source_type == "hashtag" else f"@{source_name}"
            print(f"  Searching {src_label}...")
            for attempt in range(3):
                try:
                    if source_type == "profile":
                        iterator = api.user(username=source_name).videos(count=50)
                        url_username = source_name
                    else:
                        iterator = api.hashtag(name=source_name).videos(count=50)
                        url_username = None
                    async for video in iterator:
                        if len(results) >= max_attempts:
                            break
                        if is_carousel(video):
                            data = getattr(video, "as_dict", None) or {}
                            vid = data.get("id") or getattr(video, "id", None) or ""
                            if vid in seen_ids:
                                skipped += 1
                                continue
                            full_data = None
                            if fetch_full_info:
                                try:
                                    await asyncio.sleep(2.5)
                                    u = url_username or (data.get("author") or {}).get("uniqueId") or (data.get("author") or {}).get("unique_id") or "unknown"
                                    detail = api.video(url=f"https://www.tiktok.com/@{u}/video/{vid}")
                                    full_data = await detail.info()
                                except Exception as e:
                                    print(f"  Warning: could not fetch full info for {vid}: {e}")
                            carousel = extract_carousel(video, full_data=full_data)
                            carousel = download_images(carousel)
                            results.append(carousel)
                            seen_ids.add(vid)
                            append_seen_ids([vid])
                            with open(OUTPUT_FILE, "w") as f:
                                json.dump(carousel, f, indent=2)
                            src = f"#{source_name}" if source_type == "hashtag" else f"@{source_name}"
                            print(f"  Carousel: {carousel['id']} from {src} ({len(results)}/{max_attempts})")
                            if loop_until_feasible:
                                decision = _process_with_lock(MODE)
                                if decision is None:
                                    return  # queued, exit
                                if decision == "feasible":
                                    return
                                if decision == "rejected":
                                    results.pop()
                                    print("  Rejected, trying next carousel...")
                                else:
                                    return
                    break
                except (KeyError, EmptyResponseException) as e:
                    if isinstance(e, EmptyResponseException):
                        if attempt < 2:
                            wait = (attempt + 1) * 10
                            print(f"  TikTok blocked (attempt {attempt + 1}/3). Waiting {wait}s...", file=sys.stderr)
                            await asyncio.sleep(wait)
                        else:
                            raise
                    else:
                        # KeyError: perfil/hashtag inexistente o respuesta inesperada de TikTok
                        src = f"#{source_name}" if source_type == "hashtag" else f"@{source_name}"
                        print(f"  Skipping {src}: {e}", file=sys.stderr)
                        break

    if results:
        with open(OUTPUT_FILE, "w") as f:
            json.dump(results[-1], f, indent=2)

    print(f"\nDone. Saved {len(results)} carousels to {OUTPUT_FILE}" + (f" (skipped {skipped} already seen)" if skipped else ""))

    if loop_until_feasible:
        if not results:
            print("  No carousels found.")
        else:
            print("  No feasible post found in this batch.")
        return

    # Call OpenClaw (hosted locally) to process results
    if results:
        _process_with_lock(MODE)


def _get_workspace() -> Path:
    """Workspace path. Set OPENCLAW_WORKSPACE_PATH for remote (e.g. mounted Mac mini)."""
    p = os.environ.get("OPENCLAW_WORKSPACE_PATH", "").strip()
    return Path(p).expanduser() if p else Path.home() / ".openclaw" / "workspace"


def _notify_openclaw(mode: Optional[str] = None):
    """POST to OpenClaw webhook so agent processes results_carousels.json."""
    mode = mode or MODE
    url = os.environ.get("OPENCLAW_WEBHOOK_URL", "http://127.0.0.1:18789/hooks/agent")
    token = os.environ.get("OPENCLAW_TOKEN", "")
    timeout = int(os.environ.get("OPENCLAW_WEBHOOK_TIMEOUT", "120"))
    if not token:
        _log("OpenClaw: skipped (OPENCLAW_TOKEN not set in .env)")
        print("  OpenClaw: skipped (OPENCLAW_TOKEN not set in .env)")
        return
    _log(f"OpenClaw: notifying {url}")
    # OpenClaw agent uses ~/.openclaw/workspace; copy results there so it can find them
    import shutil
    workspace = _get_workspace()
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "decision.txt").unlink(missing_ok=True)  # clear so we wait for fresh decision
    if mode == "recipes":
        (workspace / "recipe.txt").unlink(missing_ok=True)  # clear previous recipe before new run
    if OUTPUT_FILE.exists():
        shutil.copy2(OUTPUT_FILE, workspace / OUTPUT_FILE.name)
        print(f"  Copied {OUTPUT_FILE.name} to OpenClaw workspace")
    if IMAGES_DIR.exists():
        dest = workspace / "images"
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(IMAGES_DIR, dest)
        print(f"  Copied images/ to OpenClaw workspace")
    if mode == "recipes":
        message = "extract-recipe: run extract-recipe skill on results_carousels.json, analyze each image, if viable recipe extract to recipe.txt and write feasible to decision.txt, else write rejected to decision.txt."
    else:
        message = "Follow AGENTS.md: process results_carousels.json, analyze each image in photos[], decide if feasible for nutrition influencer account, write decision to decision.txt."
    try:
        import urllib.request
        req = urllib.request.Request(
            url,
            data=json.dumps({
                "message": message,
                "wakeMode": "now",
                "allowUnsafeExternalContent": True,
            }).encode(),
            headers={
                "x-openclaw-token": token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            _log(f"Notified OpenClaw (status {r.status})")
            print(f"  Notified OpenClaw (status {r.status})")
    except Exception as e:
        _log(f"OpenClaw notify FAILED: {e}")
        print(f"  Warning: could not notify OpenClaw: {e}", file=sys.stderr)
        print(f"  Check OPENCLAW_WEBHOOK_URL ({url}) and that OpenClaw is running.", file=sys.stderr)


def _load_from_queue_item(item_dir: Path):
    """Copy queued item to OUTPUT_FILE and IMAGES_DIR for processing."""
    import shutil
    src_json = item_dir / "results_carousels.json"
    src_images = item_dir / "images"
    if src_json.exists():
        shutil.copy2(src_json, OUTPUT_FILE)
    if src_images.exists():
        if IMAGES_DIR.exists():
            shutil.rmtree(IMAGES_DIR)
        shutil.copytree(src_images, IMAGES_DIR)


def _notify_wait_and_post(mode: str) -> Optional[str]:
    """Notify OpenClaw, wait for decision, post to webhook if feasible. Returns decision."""
    workspace = _get_workspace()
    _notify_openclaw(mode)
    print("  Waiting for agent decision...")
    decision = _wait_for_decision(workspace)
    if decision == "feasible":
        api_name = "recipes API" if mode == "recipes" else "influencer API"
        print(f"  Feasible! Posting to {api_name}...")
        if _post_to_webhook(mode):
            print("  Done.")
        else:
            path = _save_to_failed_queue()
            _log(f"Failed to post, saved to queue/{path.name}")
            print(f"  Failed to post, saved to {path} for retry", file=sys.stderr)
    elif decision == "rejected":
        print("  Rejected.")
    else:
        print("  Timeout waiting for decision.", file=sys.stderr)
    return decision


def _process_with_lock(mode: str) -> Optional[str]:
    """Process current carousel (or queue if locked). If locked, enqueue and return None. Else process and drain queue."""
    if _is_locked():
        path = _enqueue(mode)
        print(f"  OpenClaw busy. Queued for later: {path.name}")
        _log(f"Queued: {path.name} (mode={mode})")
        return None
    _acquire_lock()
    try:
        decision = _notify_wait_and_post(mode)
        # Procesar cola pendiente
        while True:
            next_item = _dequeue()
            if not next_item:
                break
            item_dir, item_mode = next_item
            print(f"  Processing queued item: {item_dir.name}")
            _load_from_queue_item(item_dir)
            _notify_wait_and_post(item_mode)
            _remove_from_queue(item_dir)
        return decision
    finally:
        _release_lock()


def _wait_for_decision(workspace: Path, timeout: int = 1000) -> Optional[str]:
    """Poll for decision.txt. Returns 'feasible', 'rejected', or None."""
    decision_file = workspace / "decision.txt"
    path_str = str(decision_file.resolve())
    start = time.time()
    poll_count = 0
    while time.time() - start < timeout:
        poll_count += 1
        if decision_file.exists():
            decision = decision_file.read_text(encoding="utf-8").strip().lower()
            _log(f"Found decision.txt: {decision[:50]}")
            if decision.startswith("feasible"):
                return "feasible"
            return "rejected"
        if poll_count <= 3 or poll_count % 15 == 0:  # log first 3 and every 30s
            print(f"  [poll {poll_count}] Waiting for {path_str}...", flush=True)
            _log(f"Poll {poll_count}: waiting for {path_str}")
        time.sleep(2)
    print(f"  Timeout after {poll_count} polls. Path: {path_str}", flush=True)
    _log(f"Timeout after {poll_count} polls. Path: {path_str}")
    return None


def _save_to_failed_queue() -> Path:
    """Save carousel from OUTPUT_FILE to queue/failed_*.json when POST fails. Returns path."""
    queue_dir = SCRIPT_DIR / "queue"
    queue_dir.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    path = queue_dir / f"failed_{ts}.json"
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        carousel = json.load(f)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(carousel, f, indent=2)
    return path


def _post_to_webhook(mode: str = "influencer") -> bool:
    """POST carousel to influencer or recipes webhook. Returns True on success."""
    if mode == "recipes":
        url = os.environ.get("RECIPES_WEBHOOK_URL")
        timeout = int(os.environ.get("RECIPES_WEBHOOK_TIMEOUT", "60"))
        log_name = "Recipes webhook"
    else:
        url = os.environ.get("INFLUENCER_WEBHOOK_URL")
        timeout = int(os.environ.get("INFLUENCER_WEBHOOK_TIMEOUT", "60"))
        log_name = "Influencer webhook"
    if not url:
        _log(f"{log_name}: skipped (URL not set in .env)")
        print(f"  {log_name}: skipped (URL not set in .env)")
        return False
    if not OUTPUT_FILE.exists():
        _log(f"{log_name}: no results_carousels.json to send")
        print("  No results_carousels.json to send.", file=sys.stderr)
        return False
    try:
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            carousel = json.load(f)
        influencer_name = (
            (carousel.get("author") or {}).get("username")
            or os.environ.get("INFLUENCER_NAME", "jimena")
        )
        payload = {
            "id": carousel.get("id", ""),
            "caption": carousel.get("caption", ""),
            "influencerName": influencer_name,
            "photos": [{"url": p.get("url", "")} for p in carousel.get("photos", [])],
        }
        if mode == "recipes":
            workspace = _get_workspace()
            recipe_path = workspace / "recipe.txt"
            if recipe_path.exists():
                payload["recipe"] = recipe_path.read_text(encoding="utf-8")
        # ngrok/self-signed: set SKIP_SSL_VERIFY=true in .env
        if os.environ.get("SKIP_SSL_VERIFY", "").lower() in ("true", "1", "yes"):
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            import certifi
            ctx = ssl.create_default_context(cafile=certifi.where())
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            _log(f"Posted to {log_name.lower()} (status {r.status})")
            print(f"  Posted to {log_name.lower()} (status {r.status})")
            return True
    except Exception as e:
        _log(f"Failed to POST to {log_name.lower()}: {e}")
        print(f"  Failed to POST: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    if "--url" in sys.argv:
        i = sys.argv.index("--url")
        if i + 1 < len(sys.argv):
            url = sys.argv[i + 1]
            asyncio.run(fetch_single_url(url, mode=MODE))
        else:
            print("Usage: python scraper.py --url <tiktok_url>", file=sys.stderr)
    elif "--process-only" in sys.argv and OUTPUT_FILE.exists():
        print(f"Using existing {OUTPUT_FILE} (--process-only)")
        _process_with_lock(MODE)
    elif "--init-seen" in sys.argv and OUTPUT_FILE.exists():
        # Bootstrap scraped_ids.csv from existing results_carousels.json
        with open(OUTPUT_FILE) as f:
            data = json.load(f)
        ids = [r["id"] for r in data if r.get("id")]
        append_seen_ids(ids)
        print(f"Registered {len(ids)} IDs from {OUTPUT_FILE.name} into {SEEN_IDS_FILE.name}")
    else:
        asyncio.run(main())
