import requests
import random
import csv
import time
from vpn_handler import change_server
import sys
from tqdm import tqdm
import subprocess

def get_summary(video_id:str, retry_limit=3, delay=3):
    for attempt in range(retry_limit):
        try:
            payload_url = f'https://www.youtube.com/watch?v={video_id}'
            random_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', k=22)) + '-'
            payload = {
                'deviceId' : random_id,
                'idToken': None,
                'url': payload_url,
            }
            summary_url = 'https://summarize.tech/api/summary'
            res = None

            try:
                res = requests.post(summary_url, data=payload, timeout=20)
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
                print('Changing server...')
                change_server()
                time.sleep(delay)
                continue  # Go to next attempt

            if res.status_code != 200:
                pass

            if res.status_code == 400 and 'rate limits' in res.text:
                change_server()
                time.sleep(delay)
                continue  # Go to next attempt

            if res.status_code == 500:
                time.sleep(delay)
                continue  # Go to next attempt

            return res.json()  # If the request was successful, return the result

        except Exception as e:
            print(f"Exception occurred: {e}")
            time.sleep(delay)  # If an exception occurred, wait and try again

    print("Exceeded maximum number of retries")
    return {}  # If the function hasn't returned by now, all attempts have failed


channel = sys.argv[1]

with open('successful_ids.txt', 'r') as f:
    successful_ids = f.read().splitlines()

random.shuffle(successful_ids)

csv_ids = None
with open(f'{channel}_results.csv', 'r') as f:
    reader = csv.reader(f)
    try:
        csv_ids = set([row[0] for row in reader])
    except Exception:
        csv_ids = set()

#select values from successful_ids that are not in csv_ids
successful_ids = [video_id for video_id in successful_ids if video_id not in csv_ids]

for video_id in tqdm(successful_ids):
    if video_id in csv_ids:
        #print(f'{video_id} already in csv, skipping...')
        continue
    #print(video_id)
    summary = get_summary(video_id)
    if summary is None:
        continue
    #print(summary.keys())
    #print(summary['rollups'].keys())
    #print(summary['rollups']['0'].keys())
    try:
        with open(f'{channel}_results.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([video_id, summary["title"], summary["rollups"]["0"]["summary"], summary["rollups"]])
    except Exception:
        #print('Error')
        #time.sleep(60)
        continue

subprocess.run(['mullvad', 'disconnect'])
subprocess.run(['python3', 'video_merger.py', channel])