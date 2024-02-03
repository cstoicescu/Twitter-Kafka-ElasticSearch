# Twitter-Kafka-ElasticSearch
Produce tweets on Kafka and Consumes through ElasticSearch


<img src="https://thumbs.dreamstime.com/b/under-construction-10012274.jpg" width=200%>  
          
Commands: 

Start Zookeeper:  ./bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties

Start Kafka: ./bin/windows/kafka-server-start.bat ./config/server.properties

---------------------------------------------------------------------------------
Elastic Search:  

->  GET /_cat/indices

-> GET /twitter/_search?pretty=true&q=*:*