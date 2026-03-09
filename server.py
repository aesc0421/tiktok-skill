#!/usr/bin/env python3
"""
HTTP server that accepts POST with a TikTok URL, runs the scraper and full flow.
POST /scrape with JSON: {"url": "https://www.tiktok.com/@user/photo/123"}
"""
import asyncio
import json
import os
import threading
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, request, jsonify

load_dotenv(Path(__file__).parent / ".env")

# Import after load_dotenv
from scraper import fetch_single_url

app = Flask(__name__)


def run_scraper(url: str):
    asyncio.run(fetch_single_url(url))


@app.route("/scrape", methods=["POST"])
def scrape():
    data = request.get_json(silent=True) or {}
    url = data.get("url") or request.form.get("url")
    if not url or "tiktok.com" not in url:
        return jsonify({"error": "Missing or invalid url. Send JSON: {\"url\": \"https://...\"}"}), 400
    threading.Thread(target=run_scraper, args=(url,), daemon=True).start()
    return jsonify({"status": "accepted", "url": url}), 202


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("SCRAPER_SERVER_PORT", "8080"))
    print(f"Server on http://0.0.0.0:{port}/scrape")
    app.run(host="0.0.0.0", port=port)
