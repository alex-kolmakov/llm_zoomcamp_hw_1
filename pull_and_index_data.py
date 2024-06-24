import requests 
from elasticsearch import Elasticsearch
from tqdm.auto import tqdm

docs_url = 'https://github.com/DataTalksClub/llm-zoomcamp/blob/main/01-intro/documents.json?raw=1'
docs_response = requests.get(docs_url)
documents_raw = docs_response.json()

documents = []

for course in documents_raw:
    course_name = course['course']

    for doc in course['documents']:
        doc['course'] = course_name
        documents.append(doc)

es_client = Elasticsearch('http://localhost:9200')
print(es_client.info())

index_settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "text": {"type": "text"},
            "section": {"type": "text"},
            "question": {"type": "text"},
            "course": {"type": "keyword"} 
        }
    }
}

index_name = 'course_questions'
es_client.indices.delete(index=index_name, ignore=[400, 404])
es_client.indices.create(index=index_name, body=index_settings)

for doc in tqdm(documents):
    es_client.index(index=index_name, body=doc)


query = 'How do I execute a command in a running docker container?'
search_query = {
    "query": {
        "bool": {
            "must": {
                "multi_match": {
                    "query": query,
                    "fields": ["question^4", "text"],
                    "type": "best_fields"
                }
            },
        }
    }
}

response = es_client.search(index=index_name, body=search_query)
print(f"Max score is {response['hits']['max_score']}")

filtered_search_query = {
    "size": 3,
    "query": {
        "bool": {
            "must": {
                "multi_match": {
                    "query": query,
                    "fields": ["question^4", "text"],
                    "type": "best_fields"
                }
            },
            "filter": {
                "term": {
                    "course": "machine-learning-zoomcamp"
                }
            }
        }
    }
}
response = es_client.search(index=index_name, body=filtered_search_query)

result_docs = []
for hit in response['hits']['hits']:
    result_docs.append(hit['_source']['question'])
      
print(result_docs)