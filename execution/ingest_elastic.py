import os
import json
import hashlib
import logging
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

def setup_logging():
    """Initialize logging with automatic directory creation."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"ingest_{datetime.now().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )

# Initialize logging
setup_logging()

def load_env():
    """Load environment variables from .env file in current or parent directory."""
    env_vars = {}
    
    # Try current directory first, then parent directory
    env_paths = [".env", "../.env"]
    env_file = None
    
    for path in env_paths:
        if os.path.exists(path):
            env_file = path
            break
    
    if env_file:
        with open(env_file, "r") as f:
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

def normalize_serp_api_data(raw_doc, filename, report_id):
    """
    Normalize SERP API JSON data (Google search results).
    Extracts organic results, social media posts, and all URLs.
    """
    normalized_docs = []
    
    # Extract search metadata
    search_query = raw_doc.get("search_parameters", {}).get("q", "")
    search_location = raw_doc.get("search_parameters", {}).get("location_used", "")
    
    # Process organic search results
    organic_results = raw_doc.get("organic_results", [])
    for idx, result in enumerate(organic_results):
        doc = {
            "timestamp": datetime.now().isoformat(),
            "source_file": filename,
            "source_type": "serp_api",
            "data_type": "organic_result",
            "title": result.get("title", ""),
            "body": result.get("snippet", ""),
            "url": result.get("link", ""),
            "redirect_url": result.get("redirect_link", ""),
            "displayed_link": result.get("displayed_link", ""),
            "source": result.get("source", ""),
            "position": result.get("position", idx + 1),
            "date": result.get("date", ""),
            "search_query": search_query,
            "search_location": search_location,
            "favicon": result.get("favicon", ""),
            "thumbnail": result.get("thumbnail", ""),
            "video_link": result.get("video_link", ""),
            "duration": result.get("duration", ""),
            "raw_source": result,
            "report_id": report_id,
            "_id": generate_doc_id(json.dumps(result, sort_keys=True))
        }
        normalized_docs.append(doc)
    
    # Process related questions (AI overviews, featured snippets)
    related_questions = raw_doc.get("related_questions", [])
    for idx, question in enumerate(related_questions):
        doc = {
            "timestamp": datetime.now().isoformat(),
            "source_file": filename,
            "source_type": "serp_api",
            "data_type": "related_question",
            "title": question.get("question", ""),
            "body": question.get("snippet", ""),
            "url": question.get("link", ""),
            "source": question.get("source", ""),
            "question_type": question.get("type", ""),
            "search_query": search_query,
            "search_location": search_location,
            "raw_source": question,
            "report_id": report_id,
            "_id": generate_doc_id(json.dumps(question, sort_keys=True))
        }
        normalized_docs.append(doc)
    
    # Process related searches
    related_searches = raw_doc.get("related_searches", [])
    for idx, search in enumerate(related_searches):
        doc = {
            "timestamp": datetime.now().isoformat(),
            "source_file": filename,
            "source_type": "serp_api",
            "data_type": "related_search",
            "title": search.get("query", ""),
            "url": search.get("link", ""),
            "search_query": search_query,
            "search_location": search_location,
            "raw_source": search,
            "report_id": report_id,
            "_id": generate_doc_id(json.dumps(search, sort_keys=True))
        }
        normalized_docs.append(doc)
    
    return normalized_docs

def normalize_telegram_osint_data(raw_doc, filename, report_id):
    """
    Normalize Telegram OSINT JSON data.
    """
    normalized_docs = []
    
    # Extract search metadata
    search_queries = raw_doc.get("search_queries", [])
    channels_searched = raw_doc.get("channels_searched", [])
    
    # Process messages
    messages = raw_doc.get("messages", [])
    for idx, message in enumerate(messages):
        doc = {
            "timestamp": datetime.now().isoformat(),
            "source_file": filename,
            "source_type": "telegram",
            "data_type": "message",
            "title": f"Telegram message from {message.get('sender_id', 'unknown')}",
            "body": message.get("message", ""),
            "url": f"telegram://channel/{message.get('channel', 'unknown')}/{message.get('id', 'unknown')}",
            "channel": message.get("channel", ""),
            "sender_id": message.get("sender_id", ""),
            "date": message.get("date", ""),
            "search_queries": search_queries,
            "channels_searched": channels_searched,
            "raw_source": message,
            "report_id": report_id,
            "_id": generate_doc_id(json.dumps(message, sort_keys=True))
        }
        normalized_docs.append(doc)
    
    # If no messages, create a summary document
    if not messages:
        doc = {
            "timestamp": datetime.now().isoformat(),
            "source_file": filename,
            "source_type": "telegram",
            "data_type": "search_summary",
            "title": "Telegram OSINT Search Summary",
            "body": f"Searched {len(channels_searched)} channels with {len(search_queries)} queries. No messages found.",
            "search_queries": search_queries,
            "channels_searched": channels_searched,
            "messages_found": 0,
            "raw_source": raw_doc,
            "report_id": report_id,
            "_id": generate_doc_id(json.dumps(raw_doc, sort_keys=True))
        }
        normalized_docs.append(doc)
    
    return normalized_docs

def normalize_comprehensive_report(raw_doc, filename, report_id):
    """
    Normalize comprehensive OSINT report JSON data.
    """
    normalized_docs = []
    
    # Create main report document
    doc = {
        "timestamp": datetime.now().isoformat(),
        "source_file": filename,
        "source_type": "osint_report",
        "data_type": "comprehensive_report",
        "title": f"OSINT Report: {raw_doc.get('case_name', 'Unknown')}",
        "body": json.dumps(raw_doc.get("critical_assessments", {}), indent=2),
        "investigation_id": raw_doc.get("investigation_id", ""),
        "case_name": raw_doc.get("case_name", ""),
        "investigation_phases": raw_doc.get("investigation_phases", {}),
        "critical_assessments": raw_doc.get("critical_assessments", {}),
        "bka_recommendations": raw_doc.get("bka_recommendations", []),
        "hypothesis_support": raw_doc.get("hypothesis_support", ""),
        "data_sources": raw_doc.get("data_sources", []),
        "artifacts_created": raw_doc.get("artifacts_created", []),
        "raw_source": raw_doc,
        "report_id": report_id,
        "_id": generate_doc_id(json.dumps(raw_doc, sort_keys=True))
    }
    normalized_docs.append(doc)
    
    return normalized_docs

def normalize_document(raw_doc, filename, valid_timestamp, report_id):
    """
    Normalize different raw data formats into the SOP schema.
    Detects data type and routes to appropriate normalization function.
    """
    normalized_docs = []
    
    # Detect SERP API data (Google search results)
    if isinstance(raw_doc, dict) and "search_parameters" in raw_doc and "organic_results" in raw_doc:
        logging.info(f"  Detected SERP API data in {filename}")
        return normalize_serp_api_data(raw_doc, filename, report_id)
    
    # Detect Telegram OSINT data
    if isinstance(raw_doc, dict) and ("search_queries" in raw_doc or "channels_searched" in raw_doc):
        logging.info(f"  Detected Telegram OSINT data in {filename}")
        return normalize_telegram_osint_data(raw_doc, filename, report_id)
    
    # Detect comprehensive OSINT report
    if isinstance(raw_doc, dict) and "investigation_phases" in raw_doc and "critical_assessments" in raw_doc:
        logging.info(f"  Detected comprehensive OSINT report in {filename}")
        return normalize_comprehensive_report(raw_doc, filename, report_id)
    
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

def ensure_index_exists(es, index_name):
    """
    Create index with proper mappings if it doesn't exist.
    """
    if es.indices.exists(index=index_name):
        logging.info(f"Index {index_name} already exists")
        return True
    
    # Define index mappings per SOP schema + OSINT-specific fields
    index_mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "source_file": {"type": "keyword"},
                "source_type": {"type": "keyword"},
                "data_type": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "body": {"type": "text"},
                "url": {"type": "keyword"},
                "redirect_url": {"type": "keyword"},
                "displayed_link": {"type": "keyword"},
                "uri": {"type": "keyword"},
                "source": {"type": "keyword"},
                "date": {"type": "date", "ignore_malformed": True},
                "author": {"type": "keyword"},
                "lang": {"type": "keyword"},
                "position": {"type": "integer"},
                "search_query": {"type": "text"},
                "search_location": {"type": "keyword"},
                "favicon": {"type": "keyword"},
                "thumbnail": {"type": "keyword"},
                "video_link": {"type": "keyword"},
                "duration": {"type": "keyword"},
                "question_type": {"type": "keyword"},
                "channel": {"type": "keyword"},
                "sender_id": {"type": "keyword"},
                "search_queries": {"type": "keyword"},
                "channels_searched": {"type": "keyword"},
                "messages_found": {"type": "integer"},
                "investigation_id": {"type": "keyword"},
                "case_name": {"type": "text"},
                "investigation_phases": {"type": "object", "enabled": True},
                "critical_assessments": {"type": "object", "enabled": True},
                "bka_recommendations": {"type": "text"},
                "hypothesis_support": {"type": "text"},
                "data_sources": {"type": "keyword"},
                "artifacts_created": {"type": "keyword"},
                "entities": {
                    "properties": {
                        "person": {"type": "keyword"},
                        "organization": {"type": "keyword"},
                        "location": {"type": "keyword"},
                        "position": {"type": "keyword"}
                    }
                },
                "metadata": {"type": "object", "enabled": True},
                "russian_contacts": {"type": "object", "enabled": True},
                "criminal_allegations": {"type": "object", "enabled": True},
                "intelligence_allegations": {"type": "object", "enabled": True},
                "public_statements": {"type": "text"},
                "sources": {"type": "keyword"},
                "report_id": {"type": "keyword"},
                "collection_date": {"type": "date"},
                "raw_source": {"type": "object", "enabled": False}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        }
    }
    
    try:
        es.indices.create(index=index_name, body=index_mapping)
        logging.info(f"Created index {index_name} with mappings")
        return True
    except Exception as e:
        logging.error(f"Failed to create index {index_name}: {e}")
        return False

def ingest_directory(base_dir, es, index_prefix):
    logging.info(f"Scanning {base_dir} for raw data...")
    
    docs_to_index = []
    indices_to_create = set()
    
    for root, dirs, files in os.walk(base_dir):
        # Process both raw_data and osint_construction directories
        if "raw_data" in root or "osint_construction" in root:
            # Extract report ID from path (parent of raw_data/osint_construction)
            report_id = os.path.basename(os.path.dirname(root))
            
            # Use report timestamp for index name if possible, else current
            # Format: 20260204_110300_berlin... -> 20260204_110300
            try:
                report_ts = report_id.split("_")[0] + "_" + report_id.split("_")[1]
            except IndexError:
                report_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            index_name = f"{index_prefix}{report_ts}".lower()
            indices_to_create.add(index_name)

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
                                "_op_type": "create",  # Skip duplicates instead of updating
                                "_source": doc
                            }
                            docs_to_index.append(action)
                            
                    except Exception as e:
                        logging.error(f"Failed to process {filepath}: {e}")

    # Create all required indices before ingestion
    for index_name in indices_to_create:
        ensure_index_exists(es, index_name)

    if docs_to_index:
        logging.info(f"Ingesting {len(docs_to_index)} documents into {len(indices_to_create)} index(es)...")
        try:
            success, errors = helpers.bulk(es, docs_to_index, stats_only=False, raise_on_error=False)
            
            if errors:
                logging.error(f"Bulk ingestion completed with errors. Success: {success}, Failed: {len(errors)}")
                # Log first few errors for debugging
                for i, error in enumerate(errors[:5]):
                    if 'index' in error:
                        doc_id = error['index'].get('_id', 'unknown')
                        error_msg = error['index'].get('error', {})
                        error_type = error_msg.get('type', 'unknown')
                        error_reason = error_msg.get('reason', 'unknown')
                        logging.error(f"  Failed doc {i+1} (ID: {doc_id}): {error_type} - {error_reason}")
                if len(errors) > 5:
                    logging.error(f"  ... and {len(errors) - 5} more errors")
            else:
                logging.info(f"Ingestion complete. Success: {success}, Failed: 0")
        except Exception as e:
            logging.error(f"Bulk ingestion failed: {e}")
    else:
        logging.info("No documents found to ingest.")

def main():
    try:
        env = load_env()
        
        # Debug: Show loaded environment variables
        logging.info(f"Loaded .env file with {len(env)} variables")
        es_host = env.get("ES_HOST", "http://localhost:9200")
        logging.info(f"Connecting to Elasticsearch at: {es_host}")
        
        es = get_es_client(env)
        
        if not es.ping():
            logging.error("Could not connect to Elasticsearch.")
            return

        index_prefix = env.get("ES_INDEX_PREFIX", "osint_")
        
        # Find reports directory (current or parent directory)
        reports_dir = "reports" if os.path.exists("reports") else "../reports"
        
        if not os.path.exists(reports_dir):
            logging.error("Could not find reports directory")
            return
        
        logging.info(f"Scanning {reports_dir} for data...")
        ingest_directory(reports_dir, es, index_prefix)
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()

