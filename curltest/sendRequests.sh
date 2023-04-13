
curl -d @syncRequests.json -X POST  -H "Content-Type: application/json"  http://127.0.0.1:8081/v1/api/sync

curl --data-binary @syncRequests.yaml -X POST  -H "Content-Type: application/yaml"  http://127.0.0.1:8081/v1/api/sync

curl --data-binary @syncRequests_db2.yaml -X POST  -H "Content-Type: application/yaml"  http://127.0.0.1:8081/v1/api/sync


curl -d @transRequests_db2.json -X POST  -H "Content-Type: application/json"  http://127.0.0.1:8082/v1/api/transfer
