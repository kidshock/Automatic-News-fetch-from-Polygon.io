import time
import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = 'YOUR_API_KEY'

headers = {
    'Authorization': f'Bearer {API_KEY}'
}

#Function to get news from Polygon.io API (only title, content, publisher, and published date)
def get_news(ticker, start_date, end_date):
    base_url = 'https://api.polygon.io/v2/reference/news'
    url = f'{base_url}?ticker={ticker}&published_utc.gte={start_date}&published_utc.lte={end_date}&order=desc&limit=100'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()['results']
    else:
        print(f'Error {response.status_code}: {response.json().get("error", "Unknown error")}')
        return []
# Function to remove duplicate news articles based on title
def remove_duplicates(news_data):
    seen_titles = set()
    unique_news = []
    
    for news in news_data:
        title = news.get('title', '')
        if title not in seen_titles:
            seen_titles.add(title)
            unique_news.append(news)
    
    return unique_news
# Function to fetch news over a time period with rate limit handling (for free tier)
def fetch_news(ticker, num_days):
    news_data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_days)
    api_calls = 0
    last_api_call_time = time.time()

    while start_date < end_date:
        current_end = min(start_date + timedelta(days=7), end_date)
        print(f"Fetching news from {start_date.date()} to {current_end.date()}")
        # Check if we need to wait before making the next API call
        if api_calls >= 5:
            time_since_last_call = time.time() - last_api_call_time
            if time_since_last_call < 60:
                wait_time = 60 - time_since_last_call
                print(f"Reached API limit, waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            api_calls = 0
            last_api_call_time = time.time()
        news_batch = get_news(ticker, start_date.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d'))
        news_data.extend(news_batch)
        api_calls += 1
        start_date = current_end + timedelta(days=1)
    return remove_duplicates(news_data)
# Main function to execute
def main():
    ticker = 'NVDA'  # For example, NVDA
    num_days = 365 * 2  # Two years
    news_data = fetch_news(ticker, num_days)
    # Extract required fields (title, content, publisher, and published date)
    cleaned_data = [
        {
            'title': news.get('title', ''),
            'content': news.get('description', ''),  # Description as content
            'publisher': news.get('publisher', {}).get('name', ''),  # Publisher name
            'published_date': news.get('published_utc', '')  # Published date
        }
        for news in news_data
    ]

    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(cleaned_data)
    df.to_csv('news_data.csv', index=False, encoding='utf-8')
    print(f"News data saved to 'news_data.csv'. Total articles: {len(df)}")
if __name__ == "__main__":
    main()
