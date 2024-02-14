#merge channel.csv with channel_results.csv
#%%
import pandas as pd
import numpy as np
import json
import sys
import subprocess
# %%
channel_name = sys.argv[1]
# %%
video_csv = pd.read_csv(f'../youtube_creator_videos/channels/{channel_name}/{channel_name}.csv')
summary_csv = pd.read_csv(f'{channel_name}_results.csv')
#numerical_csv = pd.read_csv(f'{channel_name}_numerical_data.csv')
# %%
video_csv
# %%
summary_csv
# %%
#add a header to summary_csv (video_id, summary, timestamp_summaries)
summary_csv.columns = ['video_id', 'title', 'summary', 'timestamp_summaries']
# %%
summary_csv
# %%
video_csv['video_id'] = video_csv['video_link'].apply(lambda x: x.split('=')[-1])
#numerical_csv['video_id'] = numerical_csv['video_link'].apply(lambda x: x.split('=')[-1])
# %%
video_csv
# %%
#combine all csvs into merged_df
#merged_df = pd.merge(pd.merge(video_csv, summary_csv, on='video_id'), numerical_csv, on='video_id')
merged_df = pd.merge(video_csv, summary_csv, on='video_id')
# %%
print(merged_df.keys())
new_column_names = {
    'video_title': 'title',
    'video_views_x': 'video_views',
    'video_link_x': 'video_link',
    'video_duration': 'video_duration',
    'video_id': 'video_id',
    'timestamp_summaries': 'timestamp',
    'views_duration_ratio': 'views_duration_ratio',
    'summary_word_count': 'summary_word_count',
    'average_word_length': 'average_word_length',
    'summary_sentiment': 'summary_sentiment',
    'title_sentiment': 'title_sentiment'
}


merged_df = merged_df.rename(columns=new_column_names)
print(merged_df.keys())
#%%
#we only want the following columns, Unnamed: 0, video_id, title, video_link, summary
merged_df = merged_df[['Unnamed: 0', 'video_id', 'title', 'video_link', 'summary', 'video_views', 'video_duration']]
# %%
merged_df = merged_df.drop_duplicates()
merged_df.to_csv(f'{channel_name}_merged.csv', index=False)
# %%
subprocess.run(['python3', 'feature_engineer.py', channel_name])

#%%
numerical_csv = pd.read_csv(f'{channel_name}_numerical_data.csv')
numerical_csv['video_id'] = numerical_csv['video_link'].apply(lambda x: x.split('=')[-1])
merged_df = pd.merge(pd.merge(video_csv, summary_csv, on='video_id'), numerical_csv, on='video_id')
new_column_names = {
    'video_title': 'title',
    'Unnamed: 0': 'video_sequence',
    'video_views_x': 'video_views',
    'video_link_x': 'video_link',
    'video_duration': 'video_duration',
    'video_id': 'video_id',
    'timestamp_summaries': 'timestamp',
    'views_duration_ratio': 'views_duration_ratio',
    'summary_word_count': 'summary_word_count',
    'average_word_length': 'average_word_length',
    'summary_sentiment': 'summary_sentiment',
    'title_sentiment': 'title_sentiment'
}

merged_df = merged_df.rename(columns=new_column_names)
merged_df = merged_df.drop_duplicates()
print(f'merged_df: {merged_df}')
# Normalize the video_views and video_sequence columns
merged_df['video_views_norm'] = (merged_df['video_views'] - merged_df['video_views'].min()) / (merged_df['video_views'].max() - merged_df['video_views'].min())
merged_df['video_sequence_norm'] = (merged_df['video_sequence'] - merged_df['video_sequence'].min()) / (merged_df['video_sequence'].max() - merged_df['video_sequence'].min())

# Create a custom_score column by subtracting normalized video_sequence from normalized video_views
merged_df['custom_score'] = merged_df['video_views_norm'] - merged_df['video_sequence_norm']

# Sort the DataFrame based on custom_score in descending order
merged_df = merged_df.sort_values(by=['custom_score'], ascending=False)

# Drop the temporary columns
merged_df.drop(columns=['video_views_norm', 'video_sequence_norm', 'custom_score'], inplace=True)
merged_df.to_csv(f'{channel_name}_merged.csv', index=False)