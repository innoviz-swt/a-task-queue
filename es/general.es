GET /

GET /my_index/_search?size=1000
{
    "query": {
        "bool": {
            "must": [
                {"match": { "type": "item" }},
                {"exists": { "field": "iid" }},
                {"exists": { "field": "timestamp" }},
            ]
        },
    },
    "fields": [
        "full_name",
        "type",
        "prop1"
    ],
    "_source": false,
    "sort": [
        {"timestamp": "asc"},
        {"iid": "asc"}
    ]
}
