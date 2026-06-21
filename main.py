import disnake
import os
import re
import json
import io
import urllib.request
import urllib.parse
import asyncio
import threading
import yt_dlp
from http.server import HTTPServer, BaseHTTPRequestHandler
from aiohttp import ClientSession, ClientTimeout
from typing import Optional, List

TOKEN = os.getenv("DISCORD_TOKEN")

intents = disnake.Intents.default()
intents.message_content = True
client = disnake.Client(intents=intents)

DISCORD_MAX_BYTES = 25 * 1024 * 1024
TIKTOK_RE = re.compile(r"https?://(?:www\.|vm\.|vt\.)?tiktok\.com/[^\s]+")

TIKWM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://www.tikwm.com/",
}
DL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.tiktok.com/",
}


async def tikwm_api(session: ClientSession, url: str) -> Optional[dict]:
    data = urllib.parse.urlencode({"url": url, "hd": "1"})
    try:
        async with session.post(
            "https://www.tikwm.com/api/",
            data=data,
            headers=TIKWM_HEADERS,
            timeout=ClientTimeout(total=10),
        ) as resp:
            result = await resp.json(content_type=None)
        if result.get("code") != 0:
            print(f"[tikwm] code={result.get('code')}")
            return None
        return result.get("data")
    except Exception as e:
        print(f"[tikwm] ошибка запроса: {e}")
        return None


TOO_LARGE = "TOO_LARGE"


async def fetch_bytes_safe(session: ClientSession, url: str) -> Optional[bytes]:
    """Скачивает файл, прерывает если больше DISCORD_MAX_BYTES."""
    try:
        async with session.get(
            url,
            headers=DL_HEADERS,
            timeout=ClientTimeout(total=60, sock_read=30),
        ) as resp:
            content_length = resp.headers.get("Content-Length")
            if content_length:
                size = int(content_length)
                if size > DISCORD_MAX_BYTES:
                    print(f"[fetch] слишком большой: {size // 1024 // 1024} МБ")
                    return TOO_LARGE
                # Размер известен и в пределах — читаем сразу всё
                return await resp.read()
            # Размер неизвестен — читаем кусками по 2МБ
            buf = io.BytesIO()
            async for chunk in resp.content.iter_chunked(2 * 1024 * 1024):
                buf.write(chunk)
                if buf.tell() > DISCORD_MAX_BYTES:
                    print("[fetch] превысил лимит при скачивании")
                    return TOO_LARGE
            return buf.getvalue()
    except Exception as e:
        print(f"[fetch] ошибка: {e}")
        return None


async def download_video(session: ClientSession, url: str) -> Optional[tuple[bytes, str]]:
    data = await tikwm_api(session, url)
    if data:
        hd_size = data.get("hd_size", 0)
        sd_size = data.get("size", 0)

        # Выбираем URL: HD если влезает, иначе SD
        if hd_size and hd_size <= DISCORD_MAX_BYTES:
            video_url = data.get("hdplay") or data.get("play")
        elif sd_size and sd_size <= DISCORD_MAX_BYTES:
            video_url = data.get("play")
        elif hd_size > DISCORD_MAX_BYTES and sd_size > DISCORD_MAX_BYTES:
            # Оба варианта точно не влезают
            return TOO_LARGE, ""
        else:
            # Размер неизвестен — пробуем SD
            video_url = data.get("play")

        if video_url:
            content = await fetch_bytes_safe(session, video_url)
            if content is TOO_LARGE:
                return TOO_LARGE, ""
            if content and len(content) > 0:
                video_id = data.get("id", "video")
                return content, f"{video_id}.mp4"

    # Запасной вариант — yt-dlp
    print("[video] tikwm не сработал, пробуем yt-dlp")
    return await asyncio.to_thread(_ytdlp_download, url)


def _ytdlp_download(url: str) -> Optional[tuple[bytes, str]]:
    class MemLogger:
        def debug(self, msg): pass
        def warning(self, msg): pass
        def error(self, msg): print(msg)

    opts = {
        "format": "best[filesize<25M][ext=mp4]/best[filesize<25M]/best[ext=mp4]/best",
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 20,
        "retries": 2,
        "logger": MemLogger(),
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            formats = info.get("formats", [info])
            # Ищем формат до 25МБ
            chosen = None
            for f in reversed(formats):
                size = f.get("filesize") or f.get("filesize_approx") or 0
                if f.get("url") and (size == 0 or size <= DISCORD_MAX_BYTES):
                    chosen = f
                    break
            if not chosen or not chosen.get("url"):
                return None
            req = urllib.request.Request(
                chosen["url"],
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.tiktok.com/"},
            )
            buf = io.BytesIO()
            with urllib.request.urlopen(req, timeout=30) as resp:
                cl = resp.headers.get("Content-Length")
                if cl and int(cl) > DISCORD_MAX_BYTES:
                    return TOO_LARGE, ""
                while True:
                    chunk = resp.read(1024 * 256)
                    if not chunk:
                        break
                    buf.write(chunk)
                    if buf.tell() > DISCORD_MAX_BYTES:
                        return TOO_LARGE, ""
            data_bytes = buf.getvalue()
            if data_bytes:
                ext = chosen.get("ext", "mp4")
                vid_id = info.get("id", "video")
                return data_bytes, f"{vid_id}.{ext}"
    except Exception as e:
        print(f"[ytdlp] ошибка: {e}")
    return None


async def download_photos(session: ClientSession, url: str) -> Optional[List[tuple[bytes, str]]]:
    data = await tikwm_api(session, url)
    if not data:
        return None
    images = data.get("images")
    if not images:
        return None
    video_id = data.get("id", "photo")
    tasks = [fetch_bytes_safe(session, img_url) for img_url in images]
    results = await asyncio.gather(*tasks)
    files = []
    for i, content in enumerate(results, 1):
        if content and len(content) <= DISCORD_MAX_BYTES:
            files.append((content, f"{video_id}_photo_{i:03d}.jpg"))
    return files if files else None


@client.event
async def on_ready():
    print(f"Бот {client.user} запущен!")
    await client.change_presence(
        activity=disnake.Activity(
            type=disnake.ActivityType.watching, name="TikTok ссылки"
        )
    )


@client.event
async def on_message(message):
    if message.author == client.user:
        return
    urls = TIKTOK_RE.findall(message.content)
    if not urls:
        return
    url = urls[0]
    async with message.channel.typing():
        async with ClientSession() as session:
            if "/photo/" in url:
                await handle_photo(message, session, url)
            else:
                await handle_video(message, session, url)


async def handle_video(message, session: ClientSession, url: str):
    result = await download_video(session, url)
    if not result:
        await message.channel.send("❌ Не удалось скачать видео.")
        return
    content, filename = result
    if content is TOO_LARGE:
        await message.channel.send("❌ Видео слишком большое для Discord (лимит 25 МБ).")
        return
    await message.channel.send(
        file=disnake.File(io.BytesIO(content), filename=filename)
    )


async def handle_photo(message, session: ClientSession, url: str):
    files_data = await download_photos(session, url)
    if not files_data:
        await message.channel.send("❌ Не удалось скачать фото.")
        return
    for i in range(0, len(files_data), 10):
        chunk = files_data[i:i + 10]
        files = [disnake.File(io.BytesIO(c), filename=n) for c, n in chunk]
        await message.channel.send(files=files)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def start_health_server():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    print(f"[health] сервер запущен на порту {port}")
    server.serve_forever()


async def run_bot():
    while True:
        try:
            await client.start(TOKEN)
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception as e:
            print(f"[bot] ошибка: {e}")
        finally:
            try:
                if not client.is_closed():
                    await client.close()
            except Exception:
                pass
        client.clear()
        print("[bot] перезапуск через 15 сек...")
        await asyncio.sleep(15)


async def main():
    await asyncio.to_thread(lambda: None)  # yield once so thread starts
    await run_bot()


if __name__ == "__main__":
    if not TOKEN:
        print("ОШИБКА: задайте DISCORD_TOKEN через переменную окружения!")
    else:
        port = int(os.getenv("PORT", 8080))
        _health_server = HTTPServer(("0.0.0.0", port), _HealthHandler)
        print(f"[health] сервер запущен на порту {port}")
        t = threading.Thread(target=_health_server.serve_forever, daemon=True)
        t.start()
        asyncio.run(main())
