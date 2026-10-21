import asyncio
import inspect
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.error import TelegramError
from telegram.request import HTTPXRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import API_ID, API_HASH, BOT_TOKEN

# MongoDB is optional: set MONGODB_URI in the shell after rotating any exposed password.
# The URI is intentionally not hard-coded into this source file.
MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "musicbot")
MONGO_CACHE_TTL = 7 * 24 * 60 * 60
mongo_client = None
music_cache = None
from telethon import TelegramClient
from telethon.sessions import StringSession
from pytgcalls import PyTgCalls, filters as call_filters
from pytgcalls.types import MediaStream, AudioQuality, StreamEnded

BASE = Path(__file__).resolve().parent
SESSION_FILE = BASE / "session.txt"

user_client = None
calls = None
bot_instance = None
current = {}
queues = {}
panels = {}
locks = {}
progress_tasks = {}
track_started = {}
paused_at = {}
paused_total = {}
suppress_end_until = {}


def get_lock(chat_id):
    return locks.setdefault(chat_id, asyncio.Lock())


def make_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸ PAUSE", callback_data="music:pause"),
         InlineKeyboardButton("▶️ RESUME", callback_data="music:resume")],
        [InlineKeyboardButton("⏭ NEXT", callback_data="music:skip"),
         InlineKeyboardButton("⏹ STOP", callback_data="music:stop")],
        [InlineKeyboardButton("☷ QUEUE", callback_data="music:queue")],
    ])


async def invoke(method, *args, **kwargs):
    result = method(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _cache_get(query):
    if music_cache is None:
        return None
    key = re.sub(r"\s+", " ", query.strip().lower())
    try:
        doc = music_cache.find_one({"_id": key})
        if not doc:
            return None
        if time.time() - float(doc.get("saved_at", 0)) > MONGO_CACHE_TTL:
            music_cache.delete_one({"_id": key})
            return None
        return doc.get("track")
    except Exception as exc:
        print("MongoDB cache read:", type(exc).__name__, str(exc))
        return None


def _cache_set(query, track):
    if music_cache is None:
        return
    key = re.sub(r"\s+", " ", query.strip().lower())
    try:
        music_cache.update_one(
            {"_id": key},
            {"$set": {"track": track, "saved_at": time.time()}},
            upsert=True,
        )
    except Exception as exc:
        print("MongoDB cache write:", type(exc).__name__, str(exc))


def _connect_mongodb():
    global mongo_client, music_cache
    if not MONGODB_URI:
        print("MongoDB cache disabled (MONGODB_URI is not set); bot will use YouTube search directly.")
        return
    try:
        from pymongo import MongoClient
        mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        mongo_client.admin.command("ping")
        music_cache = mongo_client[MONGO_DB_NAME]["search_cache"]
        print(f"MongoDB connected. Search cache enabled: {MONGO_DB_NAME}.search_cache")
    except Exception as exc:
        music_cache = None
        if mongo_client:
            try:
                mongo_client.close()
            except Exception:
                pass
        mongo_client = None
        print("MongoDB unavailable; continuing without cache:", type(exc).__name__, str(exc))


def _request_json(url, timeout=6):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 MusicBot/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def _clean_title(title):
    title = re.sub(r"\s*\((official|lyrics?|audio|video|visuali[sz]er)[^)]*\)", "", title, flags=re.I)
    title = re.sub(r"\s*\[(official|lyrics?|audio|video|visuali[sz]er)[^]]*\]", "", title, flags=re.I)
    title = re.sub(r"\s*[-|]\s*(official music video|official video|official audio|lyrics video)$", "", title, flags=re.I)
    return title.strip()


def _get_deezer_album(track_title):
    query = quote(_clean_title(track_title))
    data = _request_json(f"https://api.deezer.com/search?q={query}&limit=8")
    tracks = data.get("data") or []
    if not tracks:
        return None, None, None

    wanted = _clean_title(track_title).lower()
    wanted_words = set(re.findall(r"[a-z0-9]+", wanted))

    def score(item):
        candidate = (item.get("title") or "").lower()
        words = set(re.findall(r"[a-z0-9]+", candidate))
        overlap = len(wanted_words & words) / max(len(wanted_words), 1)
        return (2 if candidate == wanted else 0) + overlap

    item = max(tracks, key=score)
    album = item.get("album") or {}
    artist = item.get("artist") or {}
    cover = album.get("cover_xl") or album.get("cover_big") or album.get("cover_medium")
    return cover, artist.get("name"), album.get("title")


async def search_youtube(query):
    """Use MongoDB cache first, then metadata-only YouTube search and cache the result."""
    cached = await asyncio.to_thread(_cache_get, query)
    if cached:
        print(f"Search cache hit: {query}")
        return cached

    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "yt_dlp",
        "--dump-single-json", "--flat-playlist", "--no-warnings",
        "--socket-timeout", "12", "--extractor-retries", "2",
        "--skip-download", f"ytsearch1:{query}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=25)
    except asyncio.TimeoutError:
        process.kill()
        await process.communicate()
        raise RuntimeError("YouTube search timeout (25s)")

    if process.returncode != 0:
        raise RuntimeError(stderr.decode(errors="replace")[-500:] or "YouTube search failed")

    try:
        data = json.loads(stdout.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RuntimeError("Invalid YouTube search result") from exc

    entries = data.get("entries") or []
    item = next((entry for entry in entries if entry and entry.get("id")), None)
    if item is None and data.get("id") and data.get("_type") != "playlist":
        item = data
    if not item:
        raise RuntimeError("Song not found")

    video_id = item["id"]
    title = item.get("title") or query
    youtube_thumbnail = item.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

    try:
        album_cover, artist, album_title = await asyncio.wait_for(
            asyncio.to_thread(_get_deezer_album, title), timeout=7
        )
    except Exception as exc:
        print("Album artwork fallback:", type(exc).__name__, str(exc))
        album_cover, artist, album_title = None, None, None

    track = {
        "title": title,
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "thumbnail": album_cover or youtube_thumbnail,
        "youtube_thumbnail": youtube_thumbnail,
        "artist": artist or item.get("uploader") or item.get("channel") or "",
        "album": album_title or "",
        "duration": item.get("duration"),
        "video_id": video_id,
    }
    await asyncio.to_thread(_cache_set, query, track)
    return track


def duration_text(seconds):
    if not isinstance(seconds, (int, float)) or seconds < 0:
        return "--:--"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes:02d}:{seconds:02d}"


def progress_bar(chat_id, duration):
    if not isinstance(duration, (int, float)) or duration <= 0:
        return "🔴 LIVE"
    started = track_started.get(chat_id, time.monotonic())
    paused = paused_total.get(chat_id, 0.0)
    if chat_id in paused_at:
        paused += time.monotonic() - paused_at[chat_id]
    elapsed = min(max(0, int(time.monotonic() - started - paused)), int(duration))
    width = 14
    filled = min(width, int(width * elapsed / duration))
    bar = "━" * filled + "🔘" + "─" * (width - filled)
    return f"{duration_text(elapsed)}  {bar}  {duration_text(duration)}"


def panel_caption(chat_id):
    track = current.get(chat_id)
    if not track:
        return "⏹ Playback stopped."
    lines = ["🎶  𝗡𝗢𝗪 𝗣𝗟𝗔𝗬𝗜𝗡𝗚", "", f"🎵  {track['title']}"]
    if track.get("artist"):
        lines.append(f"🎤  {track['artist']}")
    if track.get("album"):
        lines.append(f"💿  {track['album']}")
    lines.extend([
        "", progress_bar(chat_id, track.get("duration")),
        f"👤  Requested by: {track.get('requester', 'Member')}",
        "🔊  Telegram Voice Chat · Audio only",
    ])
    waiting = queues.get(chat_id, [])
    if waiting:
        lines.append(f"☷  Queue: {len(waiting)} song(s)")
    return "\n".join(lines)[:1024]


async def progress_updater(chat_id):
    try:
        while chat_id in current:
            await asyncio.sleep(10)
            if chat_id not in current:
                break
            panel = panels.get(chat_id)
            if panel:
                try:
                    await panel.edit_caption(caption=panel_caption(chat_id), reply_markup=make_keyboard())
                except TelegramError:
                    pass
    except asyncio.CancelledError:
        pass


def restart_progress_updater(chat_id):
    task = progress_tasks.get(chat_id)
    if task and not task.done():
        task.cancel()
    progress_tasks[chat_id] = asyncio.create_task(progress_updater(chat_id))


def stop_progress_updater(chat_id):
    task = progress_tasks.pop(chat_id, None)
    if task and not task.done():
        task.cancel()


async def update_panel(chat_id, track):
    caption = panel_caption(chat_id)
    panel = panels.get(chat_id)
    if panel:
        try:
            await panel.edit_media(
                media=InputMediaPhoto(media=track["thumbnail"], caption=caption),
                reply_markup=make_keyboard(),
            )
            return
        except TelegramError as exc:
            print("Music panel update:", type(exc).__name__, str(exc))
    message = await bot_instance.send_photo(
        chat_id=chat_id, photo=track["thumbnail"], caption=caption,
        reply_markup=make_keyboard(),
    )
    panels[chat_id] = message


async def start_track(chat_id, track):
    # Audio only; PyTgCalls resolves the YouTube URL into an audio stream.
    stream = MediaStream(
        track["url"], AudioQuality.HIGH,
        video_flags=MediaStream.Flags.IGNORE,
    )
    await invoke(calls.play, chat_id, stream)
    current[chat_id] = track
    track_started[chat_id] = time.monotonic()
    paused_at.pop(chat_id, None)
    paused_total[chat_id] = 0.0
    try:
        await update_panel(chat_id, track)
        restart_progress_updater(chat_id)
    except Exception as exc:
        print("Music panel error:", type(exc).__name__, str(exc))


async def play_next(chat_id):
    waiting = queues.setdefault(chat_id, [])
    if not waiting:
        stop_progress_updater(chat_id)
        current.pop(chat_id, None)
        track_started.pop(chat_id, None)
        paused_at.pop(chat_id, None)
        paused_total.pop(chat_id, None)
        try:
            await invoke(calls.leave_call, chat_id)
        except Exception as exc:
            print("Leave call:", type(exc).__name__, str(exc))
        panel = panels.get(chat_id)
        if panel:
            try:
                await panel.edit_caption(caption="⏹ Queue finished — no more songs.", reply_markup=None)
            except TelegramError:
                pass
        return False
    track = waiting.pop(0)
    await start_track(chat_id, track)
    return True


async def on_stream_end(_, update: StreamEnded):
    chat_id = update.chat_id
    async with get_lock(chat_id):
        if time.monotonic() < suppress_end_until.get(chat_id, 0):
            return
        if chat_id in current:
            try:
                await play_next(chat_id)
            except Exception as exc:
                print("Auto-next error:", type(exc).__name__, str(exc))


async def is_admin(update, context):
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message
    if not chat or chat.type not in ("group", "supergroup"):
        if message:
            await message.reply_text("ℹ️ ဒီ command ကို Group ထဲမှာ အသုံးပြုပါ။")
        return False
    if not user:
        return False
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status not in ("administrator", "creator"):
            if message:
                await message.reply_text("🔒 Group Admin တွေပဲ အသုံးပြုနိုင်ပါတယ်။")
            return False
        return True
    except Exception as exc:
        print("Admin check:", type(exc).__name__, str(exc))
        if message:
            await message.reply_text("⚠️ Admin ခွင့်စစ်မရပါ။ Bot ကို Group Admin ပေးပြီး ပြန်စမ်းပါ။")
        return False


async def play_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return
    if not context.args:
        await update.effective_message.reply_text("🎵 အသုံးပြုပုံ: /play သီချင်းနာမည်")
        return

    chat_id = update.effective_chat.id
    query_text = " ".join(context.args)
    searching_message = await context.bot.send_message(
        chat_id=chat_id, text=f"🔎  SEARCHING MUSIC...\n🎧 {query_text}"
    )
    try:
        track = await search_youtube(query_text)
        track["requester"] = update.effective_user.first_name or "Member"
        async with get_lock(chat_id):
            if chat_id in current:
                queues.setdefault(chat_id, []).append(track)
                try:
                    await searching_message.edit_text(
                        f"➕ Queue ထဲ ထည့်ပြီးပါပြီ\n🎵 {track['title']}\n📜 Queue: {len(queues[chat_id])}"
                    )
                except TelegramError:
                    pass
                panel = panels.get(chat_id)
                if panel:
                    try:
                        await panel.edit_caption(caption=panel_caption(chat_id), reply_markup=make_keyboard())
                    except TelegramError:
                        pass
                return
            await start_track(chat_id, track)
        try:
            await searching_message.delete()
        except TelegramError:
            pass
    except Exception as exc:
        print("Play error:", type(exc).__name__, str(exc))
        try:
            await searching_message.edit_text(
                f"⚠️ သီချင်းဖွင့်မရပါ။\n{type(exc).__name__}: {str(exc)[:250]}"
            )
        except TelegramError:
            pass


async def pause_playback(chat_id):
    if chat_id not in current:
        return "ℹ️ လက်ရှိဖွင့်နေတဲ့ သီချင်းမရှိပါ။"
    if chat_id in paused_at:
        return "⏸ သီချင်းက Pause ဖြစ်ပြီးသားပါ။"
    await invoke(calls.pause, chat_id)
    paused_at[chat_id] = time.monotonic()
    return "⏸ သီချင်းကို ခဏရပ်ထားပါပြီ။"


async def resume_playback(chat_id):
    if chat_id not in current:
        return "ℹ️ ပြန်ဖွင့်ဖို့ သီချင်းမရှိပါ။"
    if chat_id not in paused_at:
        return "▶️ သီချင်းက ဖွင့်နေပြီးသားပါ။"
    await invoke(calls.resume, chat_id)
    paused_total[chat_id] = paused_total.get(chat_id, 0.0) + time.monotonic() - paused_at.pop(chat_id)
    return "▶️ သီချင်းကို ပြန်ဖွင့်လိုက်ပါပြီ။"


async def pause_cmd(update, context):
    if not await is_admin(update, context):
        return
    try:
        result = await pause_playback(update.effective_chat.id)
        await update.effective_message.reply_text(result)
    except Exception as exc:
        print("Pause error:", type(exc).__name__, str(exc))
        await update.effective_message.reply_text("⚠️ Pause မလုပ်နိုင်ပါ။ Voice Chat ဖွင့်ထားတာ စစ်ပါ။")


async def resume_cmd(update, context):
    if not await is_admin(update, context):
        return
    try:
        result = await resume_playback(update.effective_chat.id)
        await update.effective_message.reply_text(result)
    except Exception as exc:
        print("Resume error:", type(exc).__name__, str(exc))
        await update.effective_message.reply_text("⚠️ Resume မလုပ်နိုင်ပါ။")


async def skip_playback(chat_id):
    suppress_end_until[chat_id] = time.monotonic() + 3
    has_next = await play_next(chat_id)
    return "⏭ နောက်သီချင်းကို ဖွင့်လိုက်ပါပြီ။" if has_next else "⏭ Queue ထဲမှာ နောက်သီချင်းမရှိလို့ Playback ရပ်လိုက်ပါပြီ။"


async def skip_cmd(update, context):
    if not await is_admin(update, context):
        return
    chat_id = update.effective_chat.id
    async with get_lock(chat_id):
        try:
            result = await skip_playback(chat_id)
            await update.effective_message.reply_text(result)
        except Exception as exc:
            print("Skip error:", type(exc).__name__, str(exc))
            await update.effective_message.reply_text("⚠️ Skip မလုပ်နိုင်ပါ။")


async def stop_playback(chat_id):
    stop_progress_updater(chat_id)
    queues[chat_id] = []
    current.pop(chat_id, None)
    track_started.pop(chat_id, None)
    paused_at.pop(chat_id, None)
    paused_total.pop(chat_id, None)
    suppress_end_until[chat_id] = time.monotonic() + 3
    try:
        await invoke(calls.leave_call, chat_id)
    except Exception as exc:
        print("Stop error:", type(exc).__name__, str(exc))
    panel = panels.get(chat_id)
    if panel:
        try:
            await panel.edit_caption(caption="⏹ Playback stopped. Queue cleared.", reply_markup=None)
        except TelegramError:
            pass
    return "⏹ သီချင်းရပ်ပြီး Queue ကို ရှင်းလိုက်ပါပြီ။"


async def stop_cmd(update, context):
    if not await is_admin(update, context):
        return
    async with get_lock(update.effective_chat.id):
        result = await stop_playback(update.effective_chat.id)
    await update.effective_message.reply_text(result)


async def queue_cmd(update, context):
    if not await is_admin(update, context):
        return
    chat_id = update.effective_chat.id
    track = current.get(chat_id)
    waiting = queues.get(chat_id, [])
    if not track and not waiting:
        await update.effective_message.reply_text("📜 Queue ဗလာပါ။ /play သီချင်းနာမည် နဲ့ စတင်ပါ။")
        return
    lines = ["📜 MUSIC QUEUE"]
    if track:
        lines.append(f"▶️ Now: {track['title']}")
    if waiting:
        lines.extend(f"{i}. {song['title']}" for i, song in enumerate(waiting, 1))
    else:
        lines.append("Queue ထဲမှာ စောင့်နေတဲ့သီချင်း မရှိသေးပါ။")
    await update.effective_message.reply_text("\n".join(lines))


async def button_cmd(update, context):
    query = update.callback_query
    if not query or not query.message:
        return
    chat_id = query.message.chat.id
    user_id = query.from_user.id

    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        if member.status not in ("administrator", "creator"):
            await query.answer("🔒 Group Admin တွေပဲ အသုံးပြုနိုင်ပါတယ်။", show_alert=True)
            return
    except Exception as exc:
        print("Button permission:", type(exc).__name__, str(exc))
        await query.answer("⚠️ ခွင့်စစ်မရပါ။", show_alert=True)
        return

    action = query.data.split(":", 1)[1]
    try:
        async with get_lock(chat_id):
            if action == "pause":
                message = await pause_playback(chat_id)
            elif action == "resume":
                message = await resume_playback(chat_id)
            elif action == "skip":
                message = await skip_playback(chat_id)
            elif action == "stop":
                message = await stop_playback(chat_id)
            elif action == "queue":
                waiting = queues.get(chat_id, [])
                if waiting:
                    text = "📜 Queue:\n" + "\n".join(
                        f"{i}. {song['title']}" for i, song in enumerate(waiting, 1)
                    )
                    message = text[:190]
                else:
                    message = "📜 Queue ထဲမှာ စောင့်နေတဲ့သီချင်း မရှိပါ။"
            else:
                message = "မသိတဲ့ command ပါ။"
        await query.answer(message[:190], show_alert=True)
        panel = panels.get(chat_id)
        if panel and chat_id in current:
            try:
                await panel.edit_caption(caption=panel_caption(chat_id), reply_markup=make_keyboard())
            except TelegramError:
                pass
    except Exception as exc:
        print(f"Button {action} error:", type(exc).__name__, str(exc))
        try:
            await query.answer(f"⚠️ လုပ်ဆောင်မရပါ: {type(exc).__name__}", show_alert=True)
        except TelegramError:
            pass


async def startup(application):
    global user_client, calls
    # MongoDB cache is optional; bot still starts if MongoDB is unavailable.
    await asyncio.to_thread(_connect_mongodb)
    if not SESSION_FILE.is_file():
        raise RuntimeError("session.txt မတွေ့ပါ။")
    session = SESSION_FILE.read_text().strip()
    if not session:
        raise RuntimeError("session.txt ဗလာဖြစ်နေပါတယ်။")

    user_client = TelegramClient(
        StringSession(session), API_ID, API_HASH,
        connection_retries=5, retry_delay=2,
    )
    await user_client.connect()
    if not await user_client.is_user_authorized():
        raise RuntimeError("Telegram user session authorized မဖြစ်ပါ။")

    calls = PyTgCalls(user_client)
    await invoke(calls.start)
    calls.on_update(call_filters.stream_end())(on_stream_end)
    print("Telegram user connected.")
    print("Music Bot started.")


async def shutdown(application):
    global calls, user_client, mongo_client
    for task in list(progress_tasks.values()):
        if not task.done():
            task.cancel()
    if calls:
        try:
            await invoke(calls.stop)
        except Exception as exc:
            print("Call shutdown:", type(exc).__name__, str(exc))
    if user_client and user_client.is_connected():
        await user_client.disconnect()
    if mongo_client:
        try:
            mongo_client.close()
        except Exception as exc:
            print("MongoDB shutdown:", type(exc).__name__, str(exc))


def main():
    global bot_instance
    request = HTTPXRequest(connect_timeout=30, read_timeout=30, write_timeout=30, pool_timeout=30)
    updates_request = HTTPXRequest(connect_timeout=30, read_timeout=30, write_timeout=30, pool_timeout=30)
    app = (
        Application.builder().token(BOT_TOKEN)
        .request(request).get_updates_request(updates_request)
        .post_init(startup).post_shutdown(shutdown).build()
    )
    bot_instance = app.bot
    app.add_handler(CommandHandler("play", play_cmd))
    app.add_handler(CommandHandler("pause", pause_cmd))
    app.add_handler(CommandHandler("resume", resume_cmd))
    app.add_handler(CommandHandler("skip", skip_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("queue", queue_cmd))
    app.add_handler(CallbackQueryHandler(button_cmd, pattern=r"^music:(pause|resume|skip|stop|queue)$"))
    print("Starting Music Bot...")
    app.run_polling(bootstrap_retries=5, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()
