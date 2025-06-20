import m3u8,os,requests,json
from Crypto.Cipher import AES
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
from pymongo import ReturnDocument

# MongoDB Atlas connection URI

# MongoDB Client setup



# Headers to bypass protection
headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,te;q=0.8,hi;q=0.7",
    "cache-control": "no-cache",
    "origin": "https://www.miruro.tv",
    "pragma": "no-cache",
    "referer": "https://www.miruro.tv/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
}

def download_mega_file(m,file_name_url):
    try:
        m.download_url(file_name_url)
        return True
    except Exception as e:
        print("Error  : ",e)
        return False

def get_unprocessed_file_object(client):
    """
    Atomically fetch and lock one unprocessed document for exclusive processing.

    Args:
        client: pymongo.MongoClient instance

    Returns:
        dict or None: Locked document for processing, or None if none available.
    """
    db = client["miruai_tv_1"]
    collection = db["cloud_files"]

    query = {
        "$and": [
            {
                "$or": [
                    {"file_processed": False},
                    {"file_processed": {"$exists": False}}
                ]
            },
            {
                "$or": [
                    {"processing": False},
                    {"processing": {"$exists": False}}
                ]
            }
        ]
    }

    update = {
        "$set": {"processing": True}
    }

    doc = collection.find_one_and_update(
        query,
        update,
        return_document=ReturnDocument.AFTER
    )

    return doc


def mark_file_status(client, filename, success):
    """
    Mark a document as processed or reset its lock on failure.

    Args:
        client: pymongo.MongoClient instance
        filename: str, the filename of the document
        success: bool, True if processing succeeded, False otherwise
    """
    db = client["miruai_tv_1"]
    collection = db["cloud_files"]

    if success:
        update = {
            "$set": {"file_processed": True},
            "$unset": {"processing": ""}
        }
    else:
        update = {
            "$set": {"file_processed": False},
            "$unset": {"processing": ""}
        }

    collection.update_one({"filename": filename}, update)



def download_decrypt_merge(title, m3u8_file='video.m3u8'):
    """
    Downloads and decrypts .ts video segments from an M3U8 playlist and merges them into a single MP4 file.

    Args:
        title (str or int): The filename (without extension) for the final .mp4 file.
        m3u8_file (str): Path to the downloaded M3U8 file.
    """
    print(f"📥 Processing M3U8: {m3u8_file}")

    # Step 1: Load the m3u8 file
    playlist = m3u8.load(m3u8_file)

    # Step 2: Get AES-128 Key
    key_uri = playlist.keys[0].uri
    key_response = requests.get(key_uri, headers=headers)
    key = key_response.content

    # Step 3: Download and Decrypt Segments in Parallel
    def download_and_decrypt(segment):
        segment_url = segment.uri
        segment_data = requests.get(segment_url, headers=headers).content
        cipher = AES.new(key, AES.MODE_CBC, iv=key)
        decrypted_data = cipher.decrypt(segment_data)
        return decrypted_data

    print("⏳ Downloading and decrypting segments...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        decrypted_segments = list(tqdm(executor.map(download_and_decrypt, playlist.segments), total=len(playlist.segments)))

    # Step 4: Merge all decrypted segments into one file
    ts_file = f"{title}.ts"
    with open(ts_file, 'wb') as final_file:
        for segment in decrypted_segments:
            final_file.write(segment)

    # Step 5: Rename the final file to .mp4
    mp4_file = f"./videos/{title}.mp4"
    os.rename(ts_file, mp4_file)

    print(f"✅ Done! Video saved as '{mp4_file}'.")


def download_m3u8(url, filename="video.m3u8"):
    """
    Downloads an .m3u8 playlist file from the provided URL and saves it locally.

    Args:
        url (str): The m3u8 URL.
        filename (str): The output filename (default is 'video.m3u8').
    """
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,te;q=0.8,hi;q=0.7",
        "cache-control": "no-cache",
        "origin": "https://www.miruro.tv",
        "pragma": "no-cache",
        "referer": "https://www.miruro.tv/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
    }

    try:
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()  # Raise an error for bad status codes

        with open(filename, 'w', encoding='utf-8') as file:
            file.write(response.text)

        print(f"✅ '{filename}' downloaded successfully!")
        return True
    except Exception as e:
        print(f"❌ Failed to download m3u8 file: {e}")


def get_json_file_data(filename):
    if os.path.exists(filename):
        with open(filename,'r',encoding='utf-8')as f:
            file_data = json.load(f)
            return file_data
    return None

def process_json_file(file_name):
    os.makedirs("videos",exist_ok=True)
    with open(file_name,'r',encoding='utf-8')as f:
        file_data = json.load(f)
    for index,obj in enumerate(file_data):
        print(f"processing : {index}/{len(file_data)} ")
        v_url = obj['video_url']
        ep_num = obj['episode']
        
        if 'prxy' not in v_url:continue
        
        video_flag = download_m3u8(v_url)
        if video_flag:
            download_decrypt_merge(title=ep_num)
      
def get_bot_config(client, db_name='STORING_KEYS', collection_name='tele_bot_1', bot_uname="user_info_b_1_bot"):
    """
    Fetch a document where TELEGRAM_BOT_UNAME matches the given bot username.

    Args:
        client (MongoClient): The MongoDB client instance.
        db_name (str): Name of the database.
        collection_name (str): Name of the collection.
        bot_uname (str): Telegram bot username to search for.

    Returns:
        dict or None: Matching document or None if not found.
    """
    db = client[db_name]
    collection = db[collection_name]

    query = {"TELEGRAM_BOT_UNAME": bot_uname}
    result = collection.find_one(query)

    if result:
        print(f"✅ Found config for bot: {bot_uname}")
        return result
    else:
        print(f"❌ No config found for bot: {bot_uname}")
        return None


def fetch_session_by_name(client,session_name, output_path=None):
    """
    Fetch a session file from MongoDB by filename.
    
    Args:
        session_name (str): The name of the session file (e.g., 'session_3.session')
        output_path (str): If provided, writes the file to this path.

    Returns:
        bytes: The binary content of the session file, or None if not found.
    """
    db = client["sessionDB"]
    collection = db["sessions"]
    doc = collection.find_one({"filename": session_name})
    if doc:
        binary_data = doc["data"]
        if output_path:
            with open(output_path, "wb") as f:
                f.write(binary_data)
            print(f"Session file written to: {output_path}")
            return True
        return None
    else:
        print(f"No session file found with name: {session_name}")
        return None



#url = ''
# # Example usage:
# video_flag  =  download_m3u8(url)

# if video_flag:    

#     download_decrypt_merge(title=12)

#fetch_session_by_name("session_3.session", r"session_3.session")
