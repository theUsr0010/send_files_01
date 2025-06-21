import os
import asyncio
import nest_asyncio
import aiohttp
from telethon import TelegramClient
from telethon.tl.types import InputMediaPhotoExternal

nest_asyncio.apply()

# ✅ AniList GraphQL query
ANIME_QUERY = """
query ($id: Int) {
  Media(id: $id, type: ANIME) {
    title {
      romaji
      english
      native
    }
    description(asHtml: false)
    coverImage {
      large
    }
    siteUrl
  }
}
"""

# ✅ Fetch anime info by ID
async def fetch_anime_info(anime_id: int):
    url = "https://graphql.anilist.co"
    json_payload = {"query": ANIME_QUERY, "variables": {"id": anime_id}}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=json_payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                media = data["data"]["Media"]
                return {
                    "title": media["title"]["english"] or media["title"]["romaji"],
                    "description": media["description"],
                    "cover_image": media["coverImage"]["large"],
                    "site_url": media["siteUrl"]
                }
            else:
                raise Exception(f"GraphQL failed: {resp.status}")

# ✅ Upload a single video file
async def send_file(client, channel, file_path):
    try:
        print(f"🚀 Sending: {file_path}")
        await client.send_file(channel, file_path, caption=os.path.basename(file_path))
        print(f"✅ Sent: {file_path}")
    except Exception as e:
        print(f"❌ Failed to send {file_path}: {e}")

# ✅ Extract episode ID safely
def extract_episode_id(file_path):
    try:
        return int(os.path.basename(file_path).split('_')[1].split('.')[0])
    except Exception:
        return float('inf')  # push unparseable to the end

# ✅ Upload all files + send anime info first
async def send_all_files(session_name, videos_folder, channel, keys_data):
    client = TelegramClient(session_name, keys_data['api_id'], keys_data['api_hash'])
    await client.start()

    # Collect and sort video files
    files = [
        os.path.join(videos_folder, f)
        for f in os.listdir(videos_folder)
        if os.path.isfile(os.path.join(videos_folder, f)) and f.endswith('.mp4')
    ]
    files.sort(key=extract_episode_id)

    if files:
        # Extract anime ID from first filename
        first_file = os.path.basename(files[0])
        try:
            anime_id = int(first_file.split('_')[0])
            info = await fetch_anime_info(anime_id)
            message = f"🎬 **{info['title']}**\n\n{info['description']}\n\n🔗 [AniList]({info['site_url']})"
            await client.send_file(
                channel,
                file=InputMediaPhotoExternal(info['cover_image']),
                caption=message,
                link_preview=False
            )
            print("✅ Sent anime info")
        except Exception as e:
            print(f"❌ Failed to fetch/send anime info: {e}")

    # Send each video one-by-one
    for idx, file_path in enumerate(files, 1):
        print(f"📦 Uploading ({idx}/{len(files)}): {os.path.basename(file_path)}")
        await send_file(client, channel, file_path)

    await client.disconnect()
    print("✅ All files uploaded. Disconnected from Telegram.")

# ✅ Main wrapper
def upload_videos_to_telegram(session_name, videos_folder, channel_url, keys_data):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_all_files(session_name, videos_folder, channel_url, keys_data))
