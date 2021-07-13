from rust:slim

RUN mkdir /app 

COPY entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

RUN apt update && apt install -y libssl-dev pkg-config
COPY Cargo.toml /app/Cargo.toml
COPY src /app/src
WORKDIR /app
RUN cargo install --path . --root /app/

ENTRYPOINT ["/app/entrypoint.sh"]

EXPOSE 8080
