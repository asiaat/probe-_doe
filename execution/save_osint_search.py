import json
import os
from datetime import datetime

def save_search_result(query, method, results_summary, key_findings, sources, topic, report_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"web_search_{topic}_{timestamp}.json"
    filepath = os.path.join(report_dir, "raw_data", filename)
    
    data = {
        "search_query": query,
        "search_timestamp": datetime.now().isoformat(),
        "search_method": method,
        "results_summary": results_summary,
        "key_findings": key_findings,
        "sources": sources
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved search result to {filepath}")

if __name__ == "__main__":
    # Example usage / placeholder for manual calls
    pass
