# Google Protocol Buffers

akka-persistence-fdb uses Google Protocol Buffers to serialize data

## building the protobuf files

```
$> cd src/main/proto
$> protoc --java_out=../java/ akka-persistence-fdb.proto
```