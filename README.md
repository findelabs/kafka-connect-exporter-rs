# kafka-connect-exporter-rs

This is a simple exporter for Kafka Connect, which provides identical metrics as that of wakeful's connect exporter: https://github.com/wakeful/kafka_connect_exporter.

### Installation

Once rust has been [installed](https://www.rust-lang.org/tools/install), simply run:
```
cargo install --git https://github.com/findelabs/kafka-connect-exporter-rs.git
```

### Arguments

```
USAGE:
    kafka-connect-exporter-rs [OPTIONS] --uri <URI>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -p, --port <port>          Set port to listen on [env: LISTEN_PORT=]  [default: 8080]
    -t, --timeout <timeout>    Timeout for rest calls to connect cluster [env: HTTP_TIMEOUT=]  [default: 3]
    -u, --uri <URI>            Kafka connect exporter [env: KAFKA_CONNECT_URI=]
```
