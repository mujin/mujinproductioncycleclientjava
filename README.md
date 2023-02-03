# Mujin Production Cycle Client (HTTP/WebSocket)

1. [Mujin Controller GraphQL APIs](docs/graphql-api.md)
1. [Mujin Controller I/O Subscription and I/O Setting](docs/get-set-subscribe-io.md)


### Build

The samples can be built using the [Apache Maven](https://maven.apache.org/) build tool.

```bash
cd java/mujinproductioncycleclientjava
mvn package
```

### Run

Once built, the samples can be run using the following command. Make sure to point the URL to the Mujin controller. 

```bash
java -cp target/mujinproductioncycleclientjava.jar com.mujin.samples.OneOrder --url "http://controller1234" --username "mujin" --password "mujin"
```