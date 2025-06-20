import os
import asyncio
import nest_asyncio
from telethon import TelegramClient

nest_asyncio.apply()

# ✅ Upload a single file (no progress printing)
async def send_file(client, channel, file_path):
    try:
        print(f"🚀 Sending: {file_path}")
        await client.send_file(
            channel,
            file_path,
            caption=os.path.basename(file_path)
        )
        print(f"✅ Completed: {file_path}")
    except Exception as e:
        print(f"❌ Failed to send {file_path}: {e}")

# ✅ Upload all files in folder
async def send_all_files(session_name, videos_folder, channel, keys_data):
    client = TelegramClient(session_name, keys_data['api_id'], keys_data['api_hash'])
    await client.start()
    
    files = [
        os.path.join(videos_folder, f)
        for f in os.listdir(videos_folder)
        if os.path.isfile(os.path.join(videos_folder, f))
    ]

    # Change or remove [:2] to control how many to send
    tasks = [send_file(client, channel, f) for f in files[:2]]
    await asyncio.gather(*tasks)
    await client.disconnect()

# ✅ Wrapper to run the whole process
def upload_videos_to_telegram(session_name, videos_folder, channel_url, keys_data):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_all_files(session_name, videos_folder, channel_url, keys_data))


# keys_data = {
#     "api_id": 12345678,
#     "api_hash": "abcd1234efgh5678ijkl9012mnop3456"
# }

# upload_videos_to_telegram(
#     session_name="session_1",
#     videos_folder=r"videos",
#     channel_url="https://t.me/",
#     keys_data=keys_data
# )
