import asyncio
import aiohttp
import pandas as pd
import random
import sys
import subprocess
import os
from vpn_handler import change_server
import time


def rotate_server(delay=10):
    print('Changing server...')
    change_server()
    time.sleep(delay)

async def fetch_summary(video_id: str, session: aiohttp.ClientSession, processed: dict, retries: int = 5) -> dict:
    try:
        transcript_summarize_home = 'https://summarize.tech'
        youtube_url = f'www.youtube.com/watch?v={video_id}'
        payload_url = f'https://www.youtube.com/watch?v={video_id}'
        transcript_url = f'{transcript_summarize_home}/{youtube_url}'
        random_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', k=22)) + '-'
        payload = {
            'deviceId' : random_id,
            'idToken': None,
            'url': payload_url,
        }
        summary_url = 'https://summarize.tech/api/summary'

        if video_id in processed:
            init_status = processed[video_id]
            if init_status != 200:
                print(f"Skipping {video_id} due to non-200 init status")
                return None
        else:
            async with session.get(transcript_url) as init_res:
                processed[video_id] = init_res.status
                if init_res.status != 200:
                    print(f"Skipping {video_id} due to non-200 init status")
                    return None
                else:
                    print(f"Init status for {video_id} is 200")
                    with open('successful_ids.txt', 'a') as f:
                        f.write(f'{video_id}\n')
    except asyncio.exceptions.TimeoutError:
        if retries > 0:
            print(f"Timeout occurred. Rotating server... {retries} retries left.")
            rotate_server()
            return await fetch_summary(video_id, session, processed, retries - 1)
        else:
            print(f"All retries exhausted for {video_id}. Skipping...")
            return None

async def main(channel_name):
    processed = {}
    df = pd.DataFrame()
    df.to_csv(f'{channel_name}_results.csv', index=False)  # Create empty CSV file with header
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for link in pd.read_csv(f'../youtube_creator_videos/channels/{channel_name}/{channel_name}.csv')['video_link'].tolist():
            video_id = link.split('=')[-1]
            tasks.append(fetch_summary(video_id, session, processed))
        results = await asyncio.gather(*tasks)
        for result in results:
            if result is not None:
                df = pd.DataFrame(result)
                with open(f'{channel_name}_results.csv', 'a') as f:
                    df.to_csv(f, header=False, index=False)
    return df

if __name__ == '__main__':
    #check if successful_ids.txt exists
    if os.path.exists('successful_ids.txt'):
        os.remove('successful_ids.txt')
    channel_name = sys.argv[1]
    loop = asyncio.get_event_loop()
    rotate_server()
    json_list = loop.run_until_complete(main(channel_name))
    #subprocess.run(['mullvad', 'disconnect'])
    subprocess.run(['python3', 'summary_bulk.py', channel_name])