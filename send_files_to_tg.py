import os
import asyncio
import nest_asyncio
import aiohttp
from telethon import TelegramClient
from telethon.tl.types import InputMediaPhotoExternal

nest_asyncio.apply()

# ✅ GraphQL query to get anime info by ID
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

# ✅ Get anime info from Anilist GraphQL
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

# ✅ Upload a single file
async def send_file(client, channel, file_path):
    try:
        print(f"🚀 Sending: {file_path}")
        await client.send_file(channel, file_path, caption=os.path.basename(file_path))
        print(f"✅ Completed: {file_path}")
    except Exception as e:
        print(f"❌ Failed to send {file_path}: {e}")

# ✅ Send all files with first message (anime details)
async def send_all_files(session_name, videos_folder, channel, keys_data):
    client = TelegramClient(session_name, keys_data['api_id'], keys_data['api_hash'])
    await client.start()

    # Sort files based on episode number (filename format: animeid_episodeid.mp4)
    files = [
        os.path.join(videos_folder, f)
        for f in os.listdir(videos_folder)
        if os.path.isfile(os.path.join(videos_folder, f)) and f.endswith('.mp4')
    ]
    files.sort(key=lambda x: int(os.path.basename(x).split('_')[1].split('.')[0]))  # sort by episode_id

    if files:
        first_file = os.path.basename(files[0])
        anime_id = int(first_file.split('_')[0])

        try:
            info = await fetch_anime_info(anime_id)
            message = f"🎬 **{info['title']}**\n\n{info['description']}\n\n🔗 [AniList]({info['site_url']})"
            await client.send_file(
                channel,
                file=InputMediaPhotoExternal(info['cover_image']),
                caption=message,
                link_preview=False
            )
            print("✅ Sent anime info message")
        except Exception as e:
            print(f"❌ Failed to fetch/send anime info: {e}")

    # Send videos
    for file_path in files:
        await send_file(client, channel, file_path)

    await client.disconnect()

# ✅ Wrapper to run the upload
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
