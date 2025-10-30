from __future__ import annotations

from typing import Dict, Iterable, Iterator, List, Tuple

from elasticsearch import Elasticsearch, helpers


def get_es(host: str = "http://localhost:9200") -> Elasticsearch:
    # For local dev clusters with security disabled
    return Elasticsearch(hosts=[host], verify_certs=False)


def ensure_index(es: Elasticsearch, index_name: str) -> None:
    if es.indices.exists(index=index_name):
        return
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "english_custom": {
                        "type": "standard",
                        "stopwords": "_english_"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "english"},
                "text": {"type": "text", "analyzer": "english"},
                "source": {"type": "keyword"}
            }
        }
    }
    es.indices.create(index=index_name, mappings=mapping["mappings"], settings=mapping["settings"])  # type: ignore[arg-type]


def _doc_actions(index_name: str, docs: Iterable[Dict]) -> Iterator[Dict]:
    for d in docs:
        yield {
            "_index": index_name,
            "_id": d["doc_id"],
            "_op_type": "index",
            "doc_id": d.get("doc_id"),
            "title": d.get("title", ""),
            "text": d.get("text", ""),
            "source": d.get("source", "")
        }


def bulk_index(es: Elasticsearch, index_name: str, docs: Iterable[Dict], batch_size: int = 2000) -> Tuple[int, int]:
    success, failed = helpers.bulk(es, _doc_actions(index_name, docs), chunk_size=batch_size, raise_on_error=False)
    # helpers.bulk returns (success_count, errors) where errors is a list when raise_on_error=False.
    failed_count = len(failed) if isinstance(failed, list) else 0
    return int(success), int(failed_count)


