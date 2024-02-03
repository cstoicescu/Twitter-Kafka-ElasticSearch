# Twitter-Kafka-ElasticSearch
Produce tweets on Kafka and Consumes through ElasticSearch


[![1429546369_closed](https://github.com/cstoicescu/Twitter-Kafka-ElasticSearch/assets/53979557/45f85c52-6afd-435d-93b1-ce406fca988b)](https://www.engadget.com/twitter-shut-off-its-free-api-and-its-breaking-a-lot-of-apps-222011637.html)

## Twitter shuts down API access to 3rd party developers: 
1. https://www.theverge.com/2023/5/31/23739084/twitter-elon-musk-api-policy-chilling-academic-research      
2. https://mashable.com/article/twitter-cuts-off-api-access-apps
          
## Commands: 

Start Zookeeper:  ./bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties

Start Kafka: ./bin/windows/kafka-server-start.bat ./config/server.properties

---------------------------------------------------------------------------------
Elastic Search:  

->  GET /_cat/indices

-> GET /twitter/_search?pretty=true&q=*:*

---------------------------------------------------------------------------------
