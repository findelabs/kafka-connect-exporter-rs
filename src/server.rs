use hyper::{Body, Method, Request, Response, StatusCode};
use std::error::Error;
use reqwest::{header::{HeaderMap, HeaderValue, USER_AGENT, CONTENT_TYPE}};
use serde_json::Value;
use core::time::Duration;
//use retry::retry;
//use retry::delay::Fixed;
use backoff::ExponentialBackoff;
use backoff::future::retry;


type BoxResult<T> = Result<T,Box<dyn Error + Send + Sync>>;

#[derive(Default, Debug, Clone)]
pub struct Cluster {
    uri: String,
    timeout: u64,
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
    value: String,
    tasks: String
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

impl Task {
    async fn generate(name: &str, data: &Value) -> BoxResult<Self> {
        // Get id of connector
        let id = data.get("id").ok_or("Missing id in task")?.to_string();

        // Get state of worker
        let state = data.get("state").ok_or("Missing state in task")?.to_string().to_lowercase();

        // Get worker_id of connector
        let worker_id = data.get("worker_id").ok_or("Missing worker_id in task")?.to_string();

        // Set status of connector
        let value = get_value(state.as_ref()); 

        let task = Task {
            connector: name.to_string(),
            id,
            state,
            worker_id,
            value: value.to_string()
        };
                
        Ok(task)
    }
}

impl State {
    async fn generate(data: &Value) -> BoxResult<Self> {
        // Get connector name from data
        let name = data.get("name").ok_or("Could not convert connector to string")?;

        // Get connector doc
        let connector = data.get("connector").ok_or("Failed to find connector")?;

        // Get tasks doc
        let tasks = data.get("tasks").ok_or("Failed to find tasks")?;

        // Get state of connector
        let state = connector.get("state").ok_or("Missing state in connector")?.to_string().to_lowercase();

        // Get worker_id of connector
        let worker = connector.get("worker_id").ok_or("Missing worker_id in connector")?.to_string();

        // Set status of connector
        let value = match state.as_ref() {
            "\"running\"" => 1.to_string(),
            _ => 0.to_string()
        };

        // Get count of tasks
        let tasks = tasks.as_array().ok_or("Failed converting tasks to array")?.len().to_string();

        let state = State {
            connector: name.to_string(),
            state,
            worker,
            value,
            tasks
        };

        Ok(state)
    }
}


impl Metrics {
    pub fn new() -> Self {
        Self { states: Vec::new(), tasks: Vec::new() }
    }

    pub fn get_states(&self) -> String {
        let mut buffer = String::new();
        buffer.push_str("# HELP kafka_connect_connector_state_running is the connector running?\n");
        buffer.push_str("# TYPE kafka_connect_connector_state_running gauge\n");

        for state in &self.states {
            let message = format!("kafka_connect_connector_state_running{{connector={},state={},worker={}}} {}\n", state.connector, state.state, state.worker, state.value);
            buffer.push_str(&message);
        }
        buffer
    }

    pub fn get_tasks(&self) -> String {
        let mut buffer = String::new();

        buffer.push_str("# HELP kafka_connect_connector_tasks_state_running are connector tasks running?\n");
        buffer.push_str("# TYPE kafka_connect_connector_tasks_state_running gauge\n");
        for task in &self.tasks {
            let message = format!("kafka_connect_connector_tasks_state_running{{connector=\"{}\",id=\"{}\",state={},worker_id={}}} {}\n", task.connector, task.id, task.state, task.worker_id, task.value);
            buffer.push_str(&message);
        }
        buffer
    }

    pub fn get_connector_tasks_count(&self) -> String {
        let mut buffer = String::new();

        buffer.push_str("# HELP kafka_connect_connector_tasks_count count of tasks per connector\n");
        buffer.push_str("# TYPE kafka_connect_connector_tasks_count gauge\n");
        for task in &self.states {
            let message = format!("kafka_connect_connector_tasks_count{{connector={}}} {}\n", task.connector, task.tasks);
            buffer.push_str(&message);
        }
        buffer
    }

    pub fn get_connector_count(&self) -> String {
        let mut buffer = String::new();

        buffer.push_str("# HELP kafka_connect_connectors_count number of deployed connectors\n");
        buffer.push_str("# TYPE kafka_connect_connectors_count gauge\n");
        let message = format!("kafka_connect_connectors_count {}\n", self.states.len());
        buffer.push_str(&message);
        buffer
    }

    pub fn get_task_count(&self) -> String {
        let mut buffer = String::new();

        buffer.push_str("# HELP kafka_connect_tasks_count number of tasks\n");
        buffer.push_str("# TYPE kafka_connect_tasks_count gauge\n");
        let message = format!("kafka_connect_tasks_count {}\n", self.tasks.len());
        buffer.push_str(&message);
        buffer
    }

    pub fn up(&self) -> String {
        let value = match self.states.len() {
            0 => 0,
            _ => 1
        };

        let mut buffer = String::new();

        buffer.push_str("# HELP kafka_connect_up was the last scrape of kafka connect successful?\n");
        buffer.push_str("# TYPE kafka_connect_up gauge\n");
        let message = format!("kafka_connect_up {}\n", value);
        buffer.push_str(&message);
        buffer
    }
}

impl Cluster {
    pub fn new(uri: &str, timeout: &u64) -> BoxResult<Self> {

        let headers = Cluster::headers()?;

        let client = reqwest::Client::builder()
            .timeout(Duration::new(*timeout, 0))
            .default_headers(headers)
            .build()?;

        Ok(Self { uri: uri.to_owned(), timeout: *timeout, client })
    }

    async fn get_connectors(&self) -> BoxResult<Value> {
        let uri = format!("{}/connectors", self.uri);

        let op = || async {
            self.get_json_value(&uri).await
        };

        // Set max interval to 2, and total max time to 10 seconds
        let mut backoff = ExponentialBackoff::default();
        backoff.max_interval = Duration::new(2, 0);
        backoff.max_elapsed_time = Some(Duration::new(10, 0));

        // Retry getting connectors
        let json = retry(backoff, op).await?;
        let v: Value = serde_json::from_str(&json)?;

        Ok(v)
    }

    async fn get_status(&self, connector: &str) -> BoxResult<Value> {
        let uri = format!("{}/connectors/{}/status", self.uri, connector);

        let op = || async {
            self.get_json_value(&uri).await
        };

        // Set max interval to 2, and total max time to 10 seconds
        let mut backoff = ExponentialBackoff::default();
        backoff.max_interval = Duration::new(2, 0);
        backoff.max_elapsed_time = Some(Duration::new(10, 0));

        // Retry getting connectors
        let json = retry(backoff, op).await?;
        let v: Value = serde_json::from_str(&json)?;

        Ok(v)
    }

    async fn get_json_value(&self, uri: &str) -> Result<String, backoff::Error<reqwest::Error>> {
        match self.client.get(uri).send().await {
            Ok(m) => match m.status().as_u16() {
                200 => {
                    let body = m.text().await?;
                    log::debug!("Got body: {}", &body);
                    Ok(body)
                },
                _ => {
                    log::error!("Got non-200 status: {}", m.status().as_u16());
                    let body = m.text().await?;
                    Ok(body)
                } 
            },
            Err(e) => {
                log::error!("Error getting {}: {}", uri, e);
                return Err(backoff::Error::Transient(e))
            }
        }
    }

    async fn get_metrics(&self) -> BoxResult<Metrics> {

        // Create metrics struct
        let mut metrics = Metrics::new();

        let connectors = self.get_connectors().await?;

        for connector in connectors.as_array().ok_or("Could not transform into array")? {

            // Get name, then get that connector's status
            let name = connector.as_str().expect("Failed to get connector from array");
            let status = self.get_status(&name).await?;

            // Generate state of connector
            let state = match State::generate(&status).await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("Failed to get state of {}: {}", name, e);
                    continue
                }
            };

            // Add connector state to Metrics struct
            metrics.states.push(state);

            // Get tasks from connector, this needs to survive a task missing tasks
            let tasks = match status.get("tasks") {
                Some(t) => t,
                None => {
                    log::error!("Connector {} is missing tasks!", name);
                    continue
                }
            };
            
            for task in tasks.as_array().ok_or("Could not convert tasks to array")? {

                // Generate status of task
                let task_status = match Task::generate(&name, &task).await {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("Failed to get status of {}: {}", name, e);
                        continue
                    }
                };

                // Add task status to Metrics struct
                metrics.tasks.push(task_status);
            };

        }

        Ok(metrics)
    }

    async fn metrics(&self) -> BoxResult<String> {
        let mut buffer = String::new();

        let metrics: Metrics = self.get_metrics().await?;

        buffer.push_str(&metrics.get_states());
        buffer.push_str(&metrics.get_tasks());
        buffer.push_str(&metrics.get_connector_count());
        buffer.push_str(&metrics.get_task_count());
        buffer.push_str(&metrics.get_connector_tasks_count());
        buffer.push_str(&metrics.up());

        Ok(buffer)
    }

    fn headers() -> BoxResult<HeaderMap> {

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
        (&Method::GET, &"/health") => {
            log::debug!("Received GET to {}", &path);
            Ok(Response::new(Body::from(format!(
                "{{ \"msg\" : \"healthy\" }}",
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
        "\"running\"" => 1,
        "\"unassigned\"" => 2,
        "\"paused\"" => 3,
        _ => 0
    }
}
