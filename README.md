# kafka-connect-exporter-rs

This is a simple exporter for Kafka Connect, which provides identical metrics as that of wakeful's connect exporter: https://github.com/wakeful/kafka_connect_exporter.

### Installation

Once rust has been [installed](https://www.rust-lang.org/tools/install), simply run:
```
cargo install --git https://github.com/findelabs/kafka-connect-exporter-rs.git
```

### Usage

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

### Metrics
```
# HELP kafka_connect_connector_state_running is the connector running?
# TYPE kafka_connect_connector_state_running gauge
kafka_connect_connector_state_running{connector="test-changesets",state="running",worker="kafka-connect:8083"} 1
# HELP kafka_connect_connector_tasks_state the state of tasks. 0-failed, 1-running, 2-unassigned, 3-paused
# TYPE kafka_connect_connector_tasks_state gauge
kafka_connect_connector_tasks_state{connector="test-changesets",state="running",worker_id="kafka-connect:8083"} 1
# HELP kafka_connect_connectors_count number of deployed connectors
# TYPE kafka_connect_connectors_count gauge
kafka_connect_connectors_count 1
# HELP kafka_connect_tasks_count number of tasks
# TYPE kafka_connect_tasks_count gauge
kafka_connect_tasks_count 1
# HELP kafka_connect_up was the last scrape of kafka connect successful?
# TYPE kafka_connect_up gauge
kafka_connect_up 1
```