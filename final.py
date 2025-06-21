import os
import asyncio
import nest_asyncio
from telethon import TelegramClient
import m3u8, requests, json, shutil
from Crypto.Cipher import AES
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient

from vid_utils import (
    get_json_file_data,
    process_json_file,
    get_bot_config,
    fetch_session_by_name,
    get_unprocessed_file_object,
    mark_file_status,
    download_mega_file
)
from send_files_to_tg import upload_videos_to_telegram
from mega import Mega

nest_asyncio.apply()

# 🔧 Configs
SESSION_NAME = 'session_1.session'
VIDEO_DIR = 'videos'

# 🌐 Mongo + Mega setup
mongo_url = os.getenv("MONGO_URL")
client = MongoClient(mongo_url)

# mega_keys = os.getenv("M_TOKEN")
# if not mega_keys:
#     raise Exception("❌ M_TOKEN not set in environment.")
# user, pwd = mega_keys.split("_")
# m = Mega().login(user, pwd)

# 🔁 Processing loop
while True:
    # 🛡 Re-fetch bot config inside loop in case it changes
    keys_doc = get_bot_config(client)
    if not keys_doc:
        print("❌ Bot keys not found. Exiting.")
        break

    # Step 1: Lock a document to process
    doc = get_unprocessed_file_object(client)
    if not doc:
        print("✅ No files left to process.")
        break

    filename = doc["filename"]
    print(f"📄 Processing: {filename}")

    try:
        # Step 2: Fetch session file each time in case updated
        session_flag = fetch_session_by_name(client, SESSION_NAME, SESSION_NAME)
        if session_flag != True:
            print("⚠️ Invalid session file.")
            mark_file_status(client, filename, success=False)
            continue

        # # Step 3: Download from Mega
        # file_downloaded_flag = download_mega_file(m, doc['public_link'])
        # if not file_downloaded_flag:
        #     print("⚠️ File download failed.")
        #     mark_file_status(client, filename, success=False)
        #     continue

        # # Step 4: Read + process JSON data
        # file_data = get_json_file_data(filename)
        if len(doc['file_data'])<=0:continue
        process_json_file(doc['file_data'])

        # Step 5: Upload to Telegram
        try:
            upload_videos_to_telegram(
                SESSION_NAME,
                VIDEO_DIR,
                keys_doc['CH_NAME'],
                keys_doc
            )
        except Exception as e:
            print("❌ Upload failed:", e)
            mark_file_status(client, filename, success=False)
            continue

        # ✅ Step 6: Mark success
        mark_file_status(client, filename, success=True)

    except Exception as e:
        print("❌ General error during processing:", e)
        mark_file_status(client, filename, success=False)

    finally:
        # 🧹 Cleanup all generated media
        if os.path.exists(VIDEO_DIR):
            shutil.rmtree(VIDEO_DIR)
        for f in os.listdir():
            if f.endswith(("mp4", "m3u8", "ts")):
                os.remove(f)
