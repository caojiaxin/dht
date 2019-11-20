# DHT 网络爬虫

## config.properties 配置文件

1. server.ip=0.0.0.0                //服务器ip
2. server.port=6881                 //服务器端口
5. server.nodes.min=200             //node表最小数量
6. server.nodes.max=300             //node表最大数量
7. server.nodes.timeout=60000       //判定node最大超时时间(毫秒)
8. server.peers.core.pool.size=10   //核心线程池数量 用于请求metadata的客户端
9. server.peers.maximum.pool.size=10//最大线程池数量
10. redis.host=localhost            //redis 用于存放 announce_peer 请求数据队列
11. redis.port=6379
11. redis.password=
11. mongodb.uri=mongodb://localhost 

## 实现协议

- [x] [DHT Protocol](http://www.bittorrent.org/beps/bep_0005.html)
- [x] [Extension for Peers to Send Metadata Files](http://www.bittorrent.org/beps/bep_0009.html)
- [x] [Extension Protocol](http://www.bittorrent.org/beps/bep_0010.html)

## 运行
```
java -Xbootclasspath/a:/path/to/configDir -jar dht-1.0-SNAPSHOT-jar-with-dependencies.jar
```
