GET /

GET /my_index/_search
{
    "query": {
        "exists": {
        "field": "title"
        }
    },
    "fields": [
        "title",

    ],
    "_source": false,
}
