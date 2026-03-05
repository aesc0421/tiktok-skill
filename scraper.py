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
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from TikTokApi import TikTokApi
from TikTokApi.exceptions import EmptyResponseException

load_dotenv(Path(__file__).parent / ".env")

# Config
INPUT_FILE = Path(__file__).parent / "input.json"
OUTPUT_FILE = Path(__file__).parent / "results_carousels.json"
SEEN_IDS_FILE = Path(__file__).parent / "scraped_ids.csv"
IMAGES_DIR = Path(__file__).parent / "images"


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


def load_config():
    with open(INPUT_FILE) as f:
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


def is_carousel(video) -> bool:
    """Check if post is a carousel/photo post (not video)."""
    data = getattr(video, "as_dict", None) or {}
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
    loop_until_feasible = "--loop-until-feasible" in sys.argv
    workspace = Path.home() / ".openclaw" / "workspace"
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
                                _notify_openclaw()
                                print("  Waiting for agent decision...")
                                decision = _wait_for_decision(workspace)
                                if decision == "feasible":
                                    print("  Feasible! Posting to influencer API...")
                                    if _run_post_feasible(workspace):
                                        print("  Done.")
                                    return
                                if decision == "rejected":
                                    results.pop()
                                    print("  Rejected, trying next carousel...")
                                else:
                                    print("  Timeout waiting for decision, stopping.", file=sys.stderr)
                                    return
                    break
                except EmptyResponseException as e:
                    if attempt < 2:
                        wait = (attempt + 1) * 10
                        print(f"  TikTok blocked (attempt {attempt + 1}/3). Waiting {wait}s...", file=sys.stderr)
                        await asyncio.sleep(wait)
                    else:
                        raise

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
    _notify_openclaw()
    if results:
        workspace = Path.home() / ".openclaw" / "workspace"
        print("  Waiting for agent decision...")
        decision = _wait_for_decision(workspace)
        if decision == "feasible":
            print("  Feasible! Posting to influencer API...")
            if _run_post_feasible(workspace):
                print("  Done.")
            else:
                print("  Failed to post.", file=sys.stderr)
        elif decision == "rejected":
            print("  Rejected.")
        else:
            print("  Timeout waiting for decision.", file=sys.stderr)


def _notify_openclaw():
    """POST to OpenClaw webhook so agent processes results_carousels.json."""
    url = os.environ.get("OPENCLAW_WEBHOOK_URL", "http://127.0.0.1:18789/hooks/agent")
    token = os.environ.get("OPENCLAW_TOKEN", "")
    timeout = int(os.environ.get("OPENCLAW_WEBHOOK_TIMEOUT", "120"))
    if not token:
        print("  OpenClaw: skipped (OPENCLAW_TOKEN not set in .env)")
        return
    # OpenClaw agent uses ~/.openclaw/workspace; copy results there so it can find them
    import shutil
    workspace = Path.home() / ".openclaw" / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "decision.txt").unlink(missing_ok=True)  # clear so we wait for fresh decision
    if OUTPUT_FILE.exists():
        shutil.copy2(OUTPUT_FILE, workspace / OUTPUT_FILE.name)
        print(f"  Copied {OUTPUT_FILE.name} to OpenClaw workspace")
    if IMAGES_DIR.exists():
        dest = workspace / "images"
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(IMAGES_DIR, dest)
        print(f"  Copied images/ to OpenClaw workspace")
    try:
        import urllib.request
        req = urllib.request.Request(
            url,
            data=json.dumps({
                "message": "Follow AGENTS.md: process results_carousels.json, analyze each image in photos[], decide if feasible for nutrition influencer account, write decision to decision.txt.",
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
            print(f"  Notified OpenClaw (status {r.status})")
    except Exception as e:
        print(f"  Warning: could not notify OpenClaw: {e}", file=sys.stderr)
        print(f"  Check OPENCLAW_WEBHOOK_URL ({url}) and that OpenClaw is running.", file=sys.stderr)


def _wait_for_decision(workspace: Path, timeout: int = 1000) -> str | None:
    """Poll for decision.txt. Returns 'feasible', 'rejected', or None."""
    decision_file = workspace / "decision.txt"
    start = time.time()
    while time.time() - start < timeout:
        if decision_file.exists():
            decision = decision_file.read_text(encoding="utf-8").strip().lower()
            if decision.startswith("feasible"):
                return "feasible"
            return "rejected"
        time.sleep(2)
    return None


def _run_post_feasible(workspace: Path) -> bool:
    """Run post_feasible.py. Returns True on success."""
    script = workspace / "scripts" / "post_feasible.py"
    if not script.exists():
        print("  post_feasible.py not found", file=sys.stderr)
        return False
    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(workspace),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            if result.stdout:
                print(f"  {result.stdout.strip()}")
            return True
        if result.stderr:
            print(f"  {result.stderr.strip()}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"  Failed to run post_feasible: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    if "--process-only" in sys.argv and OUTPUT_FILE.exists():
        # Skip scraping; use existing results and notify OpenClaw
        print(f"Using existing {OUTPUT_FILE} (--process-only)")
        _notify_openclaw()
        workspace = Path.home() / ".openclaw" / "workspace"
        print("  Waiting for agent decision...")
        decision = _wait_for_decision(workspace)
        if decision == "feasible":
            print("  Feasible! Posting to influencer API...")
            if _run_post_feasible(workspace):
                print("  Done.")
            else:
                print("  Failed to post.", file=sys.stderr)
        elif decision == "rejected":
            print("  Rejected. Run scraper without --process-only to try another post.")
        else:
            print("  Timeout waiting for decision.", file=sys.stderr)
    elif "--init-seen" in sys.argv and OUTPUT_FILE.exists():
        # Bootstrap scraped_ids.csv from existing results_carousels.json
        with open(OUTPUT_FILE) as f:
            data = json.load(f)
        ids = [r["id"] for r in data if r.get("id")]
        append_seen_ids(ids)
        print(f"Registered {len(ids)} IDs from {OUTPUT_FILE.name} into {SEEN_IDS_FILE.name}")
    else:
        asyncio.run(main())
