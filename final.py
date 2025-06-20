from pymongo import MongoClient
from mega import Mega
import os,shutil


from vid_utils import get_json_file_data, process_json_file, get_bot_config, fetch_session_by_name, get_unprocessed_file_object,mark_file_status,download_mega_file
from send_files_to_tg import upload_videos_to_telegram

session_name = 'session_1.session'


mongo_url = os.getenv("MONGO_URL")
client = MongoClient(mongo_url)

keys = os.getenv("M_TOKEN")
keys = keys.split("_")
mega = Mega()
m = mega.login(keys[0],keys[1])

keys_doc = get_bot_config(client)
session_flag = fetch_session_by_name(client,session_name,session_name)

# Step 1: Get one unprocessed and unlocked file

while(True):
    
    if not keys_doc:
        print("Keys Not present..")
        break
    
    doc = get_unprocessed_file_object(client)

    if doc:
        filename = doc["filename"]
        print("📄 Processing:", filename)
        try:
            file_downloaded_flag = download_mega_file(m,filename)
            if file_downloaded_flag == False :continue
            if session_flag != True: continue
            
            file_data = get_json_file_data(filename)
            process_json_file(file_data)
            
            try:
                upload_videos_to_telegram(session_name,
                                          'videos',
                                          keys_doc['CH_NAME'],
                                          keys_doc
                                          )
            except Exception as e:
                print("Error failed to upload : ",e)
            # Step 2: If successful
            mark_file_status(client, filename, success=True)

        except Exception as e:
            print("❌ Error during processing:", e)
            # Step 3: Mark it as failed to reprocess later
            mark_file_status(client, filename, success=False)
        finally:
            shutil.rmtree("videos")
            for f in os.listdir():
                if f.endswith(("mp4","m3u8",'ts')):
                    os.remove(f)

    
    else:
        print("✅ No files left to process.")

