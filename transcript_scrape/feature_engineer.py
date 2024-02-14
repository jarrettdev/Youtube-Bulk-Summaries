import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
import sys

channel_name = sys.argv[1]

# Read the CSV data
data = pd.read_csv(f'{channel_name}_merged.csv')

# Initialize SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

# Sentiment analysis function
def get_sentiment(text):
    sentiment = analyser.polarity_scores(text)
    return sentiment['compound']

def quote_strings_with_commas(value):
    if isinstance(value, str) and ',' in value:
        return f'"{value}"'
    return value

string_columns = ['title', 'video_link', 'summary']  # Update with the names of your string columns

print(data['summary'].head())
print(data['summary'].tail())
data['summary'] = data['summary'].astype(str)

# Calculate sentiment scores for the summaries
try:
    data['summary_sentiment'] = data['summary'].apply(get_sentiment)
except Exception:
    pass
data['title_sentiment'] = data['title'].apply(get_sentiment)

# Function to convert duration to seconds
def duration_to_seconds(duration):
    #handle case where duration has hours
    if len(duration.split(':')) == 3:
        hours, minutes, seconds = duration.split(':')
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    else:
        minutes, seconds = duration.split(':')
    return int(minutes) * 60 + int(seconds)

# Convert video_duration to seconds
data['duration_seconds'] = data['video_duration'].apply(duration_to_seconds)

# Calculate the ratio between video views and duration_seconds
data['views_duration_ratio'] = data['video_views'] / data['duration_seconds']

# Extract numbers from the title (e.g., ages, years, etc.)
def extract_numbers(text):
    numbers = re.findall(r'\d+', text)
    return numbers

data['numbers_in_title'] = data['title'].apply(extract_numbers)

# Calculate the word count of the summary
data['summary_word_count'] = data['summary'].apply(lambda x: len(x.split()))

# Calculate the average word length in the summary
data['average_word_length'] = data['summary'].apply(lambda x: sum(len(word) for word in x.split()) / len(x.split()))

# Drop the original video_duration column (since we have it in seconds now)
data = data.drop(columns=['video_duration'])

# Drop the Unnamed: 0 column as it doesn't provide any useful information
data = data.drop(columns=['Unnamed: 0'])

for column in string_columns:
    data[column] = data[column].apply(quote_strings_with_commas)

# Save the updated data to a new CSV file
data.to_csv('updated_data.csv', index=False)

#only include numerical columns
numerical_columns = ['video_link', 'video_views', 'duration_seconds', 'views_duration_ratio', 'summary_word_count', 'average_word_length', 'summary_sentiment', 'title_sentiment']
data = data[numerical_columns]
data.to_csv(f'{channel_name}_numerical_data.csv', index=False)
