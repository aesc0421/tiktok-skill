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


def run_scraper(url: str, mode: str = "nutrition", skip_decision: bool = False):
    try:
        asyncio.run(fetch_single_url(url, mode=mode, skip_decision=skip_decision))
    except Exception as e:
        import traceback
        print(f"Scraper error: {e}", flush=True)
        traceback.print_exc()


def _scrape(mode: str, skip_decision: bool = False):
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or request.form.get("url") or request.form.get("text") or "").strip()
    if not url or "tiktok.com" not in url:
        return jsonify({"text": "Error: send a valid TikTok URL"}), 400
    threading.Thread(target=run_scraper, args=(url, mode, skip_decision), daemon=True).start()
    return jsonify({"text": f"Processing: {url}"}), 200


@app.route("/scrape", methods=["POST"])
def scrape():
    return _scrape("nutrition")


@app.route("/scrape-recipes", methods=["POST"])
def scrape_recipes():
    return _scrape("recipes", skip_decision=True)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("SCRAPER_SERVER_PORT", "8080"))
    print(f"Server on http://0.0.0.0:{port}/scrape")
    app.run(host="0.0.0.0", port=port)
