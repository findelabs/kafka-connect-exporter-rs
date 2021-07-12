use hyper::{Body, Method, Request, Response, StatusCode};
use std::str::from_utf8;
use std::error::Error;
use clap::ArgMatches;
use reqwest::{header::{HeaderMap, HeaderValue, USER_AGENT, CONTENT_TYPE}};
use serde_json::{json, to_value, Value};

type BoxResult<T> = Result<T,Box<dyn Error + Send + Sync>>;

#[derive(Default, Debug, Clone)]
pub struct Cluster {
    uri: String,
    timeout: u16,
    client: reqwest::Client
}

#[derive(Default, Debug)]
pub struct Metrics {
    states: Vec<State>,
    tasks: Vec<Task>
}

// Example: kafka_connect_connector_state_running{connector="myconnectorKC",state="running",worker="10.233.86.239:8083"} 1
#[derive(Default, Debug)]
pub struct State {
    connector: String,
    state: String,
    worker: String,
    value: String
}

// kafka_connect_connector_tasks_state_running{connector="myconnectorKC",id="3",state="running",worker_id="10.233.84.238:8083"} 1
#[derive(Default, Debug)]
pub struct Task {
    connector: String,
    id: String,
    state: String,
    worker_id: String,
    value: String
}

impl Metrics {
    pub fn new() -> Self {
        Self { states: Vec::new(), tasks: Vec::new() }
    }

    pub fn get_states(&self) -> String {
        let mut buffer = String::new();
        for state in &self.states {
            let message = format!("kafka_connect_connector_state_running{{connector=\"{}\",state=\"{}\",worker=\"{}\" {} }}\n", state.connector, state.state, state.worker, state.value);
            buffer.push_str(&message);
        }
        buffer
    }
}

impl Cluster {
    pub fn new(uri: &str, timeout: &u16) -> Self {
        Self { uri: uri.to_owned(), timeout: *timeout, client: reqwest::Client::new() }
    }

    async fn get_connectors(&self) -> BoxResult<Value> {
        let uri = format!("{}/connectors", self.uri);
        Ok(self.get_json_value(&uri).await?)
    }

    async fn get_status(&self, connector: &str) -> BoxResult<Value> {
        let uri = format!("{}/connectors/{}/status", self.uri, connector);
        Ok(self.get_json_value(&uri).await?)
    }

    async fn get_json_value(&self, uri: &str) -> BoxResult<Value> {
        let headers = self.headers()?;
        match self.client.get(uri).headers(headers).send().await {
            Ok(m) => match m.status().as_u16() {
                200 => {
                    let body = m.text().await?;
                    log::debug!("Got body: {}", &body);
                    let v: Value = serde_json::from_str(&body)?;
                    Ok(v)
                },
                _ => {
                    log::error!("Got non-200 status: {}", m.status().as_u16());
                    let body = m.text().await?;
                    let v: Value = serde_json::from_str(&body)?;
                    Ok(v)
                } 
            },
            Err(e) => {
                log::error!("Caught error posting: {}", e);
                return Err(Box::new(e))
            }
        }
    }

    async fn get_metrics(&self) -> BoxResult<Metrics> {

        let mut metrics = Metrics::new();

        let connectors = self.get_connectors().await?;

        for connector in connectors.as_array().ok_or("Could not transform into array")? {
            let connector = connector.as_str().ok_or("Could not convert connector to string")?;
            let status = self.get_status(&connector).await?;

            let connector_status = status.get("connector").ok_or("Failed to find connector in status output")?;
            let connector_tasks = status.get("tasks").ok_or("Failed to find tasks in status output")?;

            let state = State {
                connector: connector.to_string(),
                state: connector_status.get("state").ok_or("Missing state in connector status")?.to_string().to_lowercase(),
                worker: connector_status.get("worker_id").ok_or("Missing worker_id in connector status")?.to_string(),
                value: get_value(&connector_status.get("state").ok_or("Missing state in connector status")?.to_string()).to_string(),
            };

            // Add connector state to Metrics struct
            metrics.states.push(state);

            for task in connector_tasks.as_array().ok_or("Could not get tasks from connector")? {

                let task = Task {
                    connector: connector.to_string(),
                    id: task.get("id").ok_or("Missing id in task")?.to_string(),
                    state: task.get("state").ok_or("Missing state in task")?.to_string().to_lowercase(),
                    worker_id: task.get("worker_id").ok_or("Missing worker_id in task")?.to_string(),
                    value: get_value(&task.get("state").ok_or("Missing state in connector status")?.to_string()).to_string(),
                };
                
                metrics.tasks.push(task);
            };

        }

        Ok(metrics)
    }

    async fn metrics(&self) -> BoxResult<String> {
        let metrics: Metrics = self.get_metrics().await?;

        let string = metrics.get_states();

        Ok(string)
    }

    fn headers(&self) -> BoxResult<HeaderMap> {

        // Create HeaderMap
        let mut headers = HeaderMap::new();

        // Add all headers
        headers.insert(USER_AGENT, HeaderValue::from_str("kafka-connecto-exporter-rs").unwrap());
        headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());

        // Return headers
        Ok(headers)
    }
}

// This is the main handler, to catch any failures in the echo fn
pub async fn main_handler(
    req: Request<Body>,
    cluster: Cluster
) -> BoxResult<Response<Body>> {
    match echo(req, cluster).await {
        Ok(s) => {
            log::debug!("Handler got success");
            Ok(s)
        }
        Err(e) => {
            log::error!("Handler caught error: {}", e);
            let mut response = Response::new(Body::from(format!("{{\"error\" : \"{}\"}}", e)));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(response)
        }
    }
}

// This is our service handler. It receives a Request, routes on its
// path, and returns a Future of a Response.
async fn echo(req: Request<Body>, cluster: Cluster) -> BoxResult<Response<Body>> {

    // Get path
    let path = &req.uri().path();

    match (req.method(), path) {
        (&Method::GET, &"/metrics") => {
            let path = req.uri().path();
            log::info!("Received GET to {}", &path);

            let metrics = cluster.metrics().await?;
        
            Ok(Response::new(Body::from(format!(
                "{}", metrics
            ))))
        },
        _ => {
            Ok(Response::new(Body::from(format!(
                "{{ \"msg\" : \"{} is not a known path\" }}",
            path
            ))))
        }
    }
}

fn get_value(state: &str) -> u16 {
    match state {
        "\"FAILED\"" => 0,
        "\"RUNNING\"" => 1,
        "\"UNASSIGNED\"" => 2,
        "\"PAUSED\"" => 3,
        _ => {
            log::info!("Failed matching state, found {}", state);
            0
        }
    }
}
