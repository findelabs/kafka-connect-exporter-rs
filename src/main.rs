use chrono::Local;
use clap::{crate_version, App, Arg};
use env_logger::{Builder, Target};
use log::LevelFilter;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Server};
use std::io::Write;
use std::error::Error;

mod server;

type BoxResult<T> = Result<T,Box<dyn Error + Send + Sync>>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> BoxResult<()> {
    let opts = App::new("kafka-connect-exporter-rs")
        .version(crate_version!())
        .author("Daniel F. <dan@findelabs.com>")
        .about("Main findereport site generator")
        .arg(
            Arg::with_name("uri")
                .short("u")
                .long("uri")
                .required(true)
                .value_name("URI")
                .env("MONGODB_URI")
                .help("MongoDB URI")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Set port to listen on")
                .required(false)
                .env("LISTEN_PORT")
                .default_value("8080")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("timeout")
                .short("t")
                .long("timeout")
                .help("Timeout for rest calls to connect cluster")
                .required(false)
                .env("HTTP_TIMEOUT")
                .default_value("3")
                .takes_value(true),
        )
        .get_matches();

    // Initialize log Builder
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{{\"date\": \"{}\", \"level\": \"{}\", \"message\": \"{}\"}}",
                Local::now().format("%Y-%m-%dT%H:%M:%S:%f"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();

    // Read in config file
    let uri = &opts.value_of("uri").unwrap();
    let port: u16 = opts.value_of("port").unwrap().parse().unwrap_or_else(|_| {
        log::error!("specified port isn't in a valid range, setting to 8080");
        8080
    });

    let timeout: u16 = opts.value_of("timeout").unwrap().parse().unwrap_or_else(|_| {
        log::error!("timeout not in proper range, defaulting to 3");
        3
    });

    let cluster = server::Cluster::new(&uri, &timeout);

    let addr = ([0, 0, 0, 0], port).into();
    let service = make_service_fn(move |_| {
        let cluster = cluster.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                server::main_handler(req, cluster.clone())
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);

    println!(
        "Starting json-bucket:{} on http://{}",
        crate_version!(),
        addr
    );

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }

    Ok(())
}
