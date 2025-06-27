from elasticsearch import Elasticsearch


esuser = "elastic"
espassword = "aG89E4AMmasMPJp4"
eshost = "11.0.0.145"
esport = "9200"

es = Elasticsearch(
     f"http://{esuser}:{espassword}@{eshost}:{esport}",  # Elasticsearch endpoint
     #ca_certs='es.cert',
     verify_certs=False,
     request_timeout=120
)
print(es.ping())
mapping = {
    "mappings": {
        "properties": {
            "@timestamp": {
                "type": "date"
            },
            "actor": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "producer": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "director": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "title": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "title_brief": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "keywords": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "message": {
                "type": "text"
            }
        }
    }
}
index_name = "tata-play-index-test"

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)


def should_search(input):
    # print(input)
    response = es.search(
        index="tata-play-index-test",
        query={
            "bool": {
                "should": [
                    {"term": {"actor.keyword": input}},
                    {"term": {"producer.keyword": input}},
                    {"term": {"director.keyword": input}},
                    {"term": {"title_brief.keyword": input}},
                    {"term": {"title.keyword": input}},
                    {"term": {"keywords.keyword": input}},
                    {
                        "multi_match": {
                            "query": input,
                            "fields": [
                                "actor", "producer", "director", "title",
                                "title_brief", "keywords"
                            ],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    }
                ]
            }
        }
    )
    response = response["hits"]["hits"]
    
    ret_response = []
    for hit in response:
        # Using .get() to safely access fields
        title_brief = hit["_source"].get("title_brief", "N/A")  # Default to "N/A" if key doesn't exist
        producer = hit["_source"].get("producer", "N/A")
        director = hit["_source"].get("director", "N/A")
        actor = hit["_source"].get("actor", "N/A")
        keywords = hit["_source"].get("keywords", "N/A")
        language = hit["_source"].get("language", "N/A")
        summary = hit["_source"].get("summary_short", "N/A")

        ret_response.append({
            "title": title_brief,
            "producer": producer,
            "director": director,
            "actor": actor,
            "keywords": keywords,
            "language": language,
            "summary": summary
        })

    return ret_response


res = should_search("Hum Hindustani")
print([res[i]['title'] for i in range(0, len(res))])