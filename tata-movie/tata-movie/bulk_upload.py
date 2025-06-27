from elasticsearch import Elasticsearch,helpers
import json
import time

esuser = "elastic"
espassword = "aG89E4AMmasMPJp4"
eshost = "11.0.0.145"
esport = "9200"
index_name = "tata-play-index-test" 
es = Elasticsearch(
     f"http://{esuser}:{espassword}@{eshost}:{esport}",  # Elasticsearch endpoint
     #ca_certs='es.cert',
     verify_certs=False,
     request_timeout=120
)
def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)  # Load entire list of dicts

# Index data in smaller batches
def bulk_index_data(es, data, chunk_size=500):
    for i in range(0, len(data), chunk_size):
        batch = data[i : i + chunk_size]
        bulk_data = [
            {
                "_index": index_name,
                "_id": str(doc["vod_id"]),  # Use `vod_id` as unique ID
                "_source": doc
            }
            for doc in batch
        ]
        try:
            helpers.bulk(es, bulk_data)
            time.sleep(0.1)
            es.indices.refresh()
            time.sleep(0.1)
            print("batch completed")
            print(f"Indexed {i + len(batch)}/{len(data)} documents successfully.")
        except Exception as e:
            print(f"Error indexing batch {i}-{i+chunk_size}: {e}")

# Main execution
file_path = r"C:\Users\admin\Documents\tata-movie\tata-movie\test_es.json"  # Replace with actual path 
data = load_json(file_path)
bulk_index_data(es, data, chunk_size=500)  # Change chunk_size as needed

print("FINISH")