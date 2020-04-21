# DHT 网络爬虫

## config.properties 配置文件

1. server.ip=0.0.0.0                //服务器ip
2. server.port=6881                 //服务器端口
5. server.nodes.min=200             //node表最小数量
6. server.nodes.max=5000            //node表最大数量
7. server.nodes.timeout=60000       //判定node最大超时时间(毫秒)
8. server.peers.core.pool.size=10   //核心线程池数量 用于请求metadata的客户端
9. server.peers.maximum.pool.size=10//最大线程池数量
10. server.findNode.interval=60     //findNode操作间隔(秒)
11. server.ping.interval=300        //ping操作间隔(秒)
12. server.removeNode.interval=300  //removeNode操作间隔(秒)
13. server.peerRequest.interval=5   //线程池信息显示间隔(秒)
14. mongodb.uri=mongodb://localhost 

## 实现协议

- [x] [DHT Protocol](http://www.bittorrent.org/beps/bep_0005.html)
- [x] [Extension for Peers to Send Metadata Files](http://www.bittorrent.org/beps/bep_0009.html)
- [x] [Extension Protocol](http://www.bittorrent.org/beps/bep_0010.html)

## 运行
jar包和config.properties配置文件要在同一目录
```shell script
java -jar dht-1.0-SNAPSHOT-jar-with-dependencies.jar
```
