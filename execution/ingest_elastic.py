import os
import json
import hashlib
import logging
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/ingest_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

def load_env():
    env_vars = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    try:
                        key, value = line.split("=", 1)
                        env_vars[key] = value
                    except ValueError:
                        pass
    return env_vars

def get_es_client(env):
    host = env.get("ES_HOST", "http://localhost:9200")
    user = env.get("ES_USER", "")
    password = env.get("ES_PASS", "")

    if user and password:
        return Elasticsearch([host], basic_auth=(user, password))
    else:
        return Elasticsearch([host])

def generate_doc_id(content_str):
    return hashlib.sha256(content_str.encode('utf-8')).hexdigest()

def normalize_document(raw_doc, filename, valid_timestamp, report_id):
    """
    Normalize different raw data formats into the SOP schema.
    Assumes list of items (NewsAPI) or single dict.
    """
    normalized_docs = []
    
    # Handle list of articles (NewsAPI format)
    if isinstance(raw_doc, list):
        items = raw_doc
    # Handle wrapped "articles" object
    elif isinstance(raw_doc, dict) and "articles" in raw_doc and "results" in raw_doc["articles"]:
         items = raw_doc["articles"]["results"]
    elif isinstance(raw_doc, dict):
        items = [raw_doc]
    else:
        logging.warning(f"Unknown JSON structure in {filename}")
        return []

    for item in items:
        # Determine Source Type based on content
        source_type = "unknown"
        data_type = "unknown"
        
        # NewsAPI detection
        if "source" in item and "uri" in item:
            source_type = "news"
            data_type = "article"
        # Generic heuristic
        elif "url" in item:
             source_type = "web"
             data_type = "page"

        doc = {
            "timestamp": datetime.now().isoformat(),
            "source_file": filename,
            "source_type": source_type,
            "data_type": data_type,
            "title": item.get("title", ""),
            "body": item.get("body", item.get("content", "")),
            "url": item.get("url", item.get("link", "")),
            "raw_source": item,
            "report_id": report_id,
            "_id": generate_doc_id(json.dumps(item, sort_keys=True))
        }
        normalized_docs.append(doc)
        
    return normalized_docs

def ingest_directory(base_dir, es, index_prefix):
    logging.info(f"Scanning {base_dir} for raw data...")
    
    docs_to_index = []
    
    for root, dirs, files in os.walk(base_dir):
        if "raw_data" in root:
            # Extract report ID from path (parent of raw_data)
            report_id = os.path.basename(os.path.dirname(root))
            
            # Use report timestamp for index name if possible, else current
            # Format: 20260204_110300_berlin... -> 20260204_110300
            try:
                report_ts = report_id.split("_")[0] + "_" + report_id.split("_")[1]
            except IndexError:
                report_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            index_name = f"{index_prefix}{report_ts}".lower()

            for file in files:
                if file.endswith(".json"):
                    filepath = os.path.join(root, file)
                    logging.info(f"Processing {filepath}")
                    
                    try:
                        with open(filepath, "r") as f:
                            raw_content = json.load(f)
                            
                        normalized = normalize_document(raw_content, filepath, report_ts, report_id)
                        
                        for doc in normalized:
                            action = {
                                "_index": index_name,
                                "_id": doc.pop("_id"), # Use generated ID
                                "_source": doc
                            }
                            docs_to_index.append(action)
                            
                    except Exception as e:
                        logging.error(f"Failed to process {filepath}: {e}")

    if docs_to_index:
        logging.info(f"Ingesting {len(docs_to_index)} documents...")
        success, failed = helpers.bulk(es, docs_to_index, stats_only=True)
        logging.info(f"Ingestion complete. Success: {success}, Failed: {failed}")
    else:
        logging.info("No documents found to ingest.")

def main():
    try:
        env = load_env()
        es = get_es_client(env)
        
        if not es.ping():
            logging.error("Could not connect to Elasticsearch.")
            return

        index_prefix = env.get("ES_INDEX_PREFIX", "osint_")
        ingest_directory("reports", es, index_prefix)
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
