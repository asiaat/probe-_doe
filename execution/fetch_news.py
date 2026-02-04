import os
import sys
import json
import logging
import requests
from datetime import datetime

# Setup logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
date_str = datetime.now().strftime("%Y%m%d")
log_file = f"{log_dir}/exec_{date_str}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_execution(tool_name, params, status, error=None):
    logging.info(f"Tool: {tool_name} | Params: {params} | Status: {status} | Error: {error}")

def load_env():
    env_vars = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    env_vars[key] = value
    return env_vars

def fetch_news(api_key, query_term, count=100):
    url = "http://eventregistry.org/api/v1/article/getArticles"
    
    # Query for "Vulkangruppe" AND ("investigation" OR "Ermittlung")
    # Using JSON structure for complex query
    payload = {
        "action": "getArticles",
        "keyword": query_term,
        "articlesCount": count,
        "resultType": "articles",
        "apiKey": api_key,
        "lang": ["eng", "deu"] # English and German
    }
    
    try:
        logging.info(f"Sending request to {url} with query '{query_term}'")
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log_execution("fetch_news.py", {"query": query_term}, "FAILURE", str(e))
        print(f"Error fetching news: {e}")
        return None

import argparse

def main():
    parser = argparse.ArgumentParser(description="Fetch news articles using NewsAPI.ai")
    parser.add_argument("--query", help="Search query for news articles", default="Vulkangruppe")
    parser.add_argument("--output-dir", help="Directory to save the output JSON", default=".tmp")
    args = parser.parse_args()

    env = load_env()
    api_key = env.get("NEWS_API_KEY")
    
    if not api_key:
        print("Error: NEWS_API_KEY not found in .env")
        log_execution("fetch_news.py", {}, "FAILURE", "Missing API Key")
        return

    query = args.query
    print(f"Fetching up to 100 articles for '{query}'...")
    
    data = fetch_news(api_key, query, count=100)
    
    if data and "articles" in data:
        articles = data["articles"]["results"]
        count = len(articles)
        print(f"Successfully fetched {count} articles.")
        
        # Save to output directory with timestamp per SOP
        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sanitized_query = "".join(c if c.isalnum() else "_" for c in query)[:30]
        output_file = f"{args.output_dir}/news_{sanitized_query}_{timestamp}.json"
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)
            
        print(f"Saved to {output_file}")
        log_execution("fetch_news.py", {"query": query, "count": count, "output": output_file}, "SUCCESS")
    else:
        print("No articles found or API error.")
        error_msg = data.get("error", "Unknown error") if data else "No data"
        log_execution("fetch_news.py", {"query": query}, "FAILURE", error_msg)

if __name__ == "__main__":
    main()
