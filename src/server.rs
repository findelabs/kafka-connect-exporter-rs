use hyper::{Body, Method, Request, Response, StatusCode};
use std::str::from_utf8;
use std::error::Error;
use clap::ArgMatches;
use reqwest::{header::{HeaderMap, HeaderValue, USER_AGENT, CONTENT_TYPE}};

type BoxResult<T> = Result<T,Box<dyn Error + Send + Sync>>;

pub struct Cluster {
    uri: String,
    timeout: u16
}

#[derive(Default)]
pub struct Metrics {
    kafka_cluster_id: String,
    connectors: Vec<Connector>
}

#[derive(Default)]
pub struct Connector {
    name: String,
    connector: Head,
    tasks: Vec<Task>
}

#[derive(Default)]
pub struct Head {
    state: String,
    worker_id: String
}

#[derive(Default)]
pub struct Task {
    id: u16,
    state: String,
    worker_id: String,
    trace: Option<String>
}

impl Cluster {
    pub fn new(uri: &str, timeout: &u16) -> Self {
        Self { uri: uri.to_owned(), timeout: *timeout }
    }

    async fn get_connectors(&self) -> BoxResult<Metrics> {
        let uri = format!("{}/connectors", self.uri);
        let headers = self.headers()?;

        let client = reqwest::Client::new()
            .get(&uri)
            .headers(headers);

        match client.send().await {
            Ok(m) => match m.status().as_u16() {
                429 => Ok("Hit rate limiter".to_owned()),
                200 => {
                    let body = match m.text().await {
                        Ok(b) => Ok(b),
                        Err(e) => return Err(Box::new(e))
                    };
                    log::info!("Got 200, body: {}", body?);
                    Ok("Get Ok".to_owned())
                },
                _ => Ok("Got weird result".to_owned())
            },
            Err(e) => {
                log::error!("Caught error posting: {}", e);
                return Err(Box::new(e))
            }
        }

//        Ok(Metrics::default())
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
    opts: ArgMatches<'_>,
    req: Request<Body>
) -> BoxResult<Response<Body>> {
    match echo(opts, req).await {
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
async fn echo(opts: ArgMatches<'_>, req: Request<Body>) -> BoxResult<Response<Body>> {

    // Get path
    let path = &req.uri().path();

    match (req.method(), path) {
        (&Method::GET, &"/metrics") => {
            let path = req.uri().path();
            log::info!("Received GET to {}", &path);
        
            Ok(Response::new(Body::from(format!(
                "{{ \"msg\" : \"Display /metrics\" }}",
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

