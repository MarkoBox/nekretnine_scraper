CREATE TABLE staging (
    id serial PRIMARY KEY,
    add_id VARCHAR(20),
    add_json json,
    url VARCHAR(200),
    project VARCHAR(50),
    spider VARCHAR(50),
    server VARCHAR(50),
    date TIMESTAMP
);

CREATE TABLE urls_to_dl (
    id serial PRIMARy KEY,
    add_id VARCHAR(20),
    add_price VARCHAR(20),
    url VARCHAR(200),
    project VARCHAR(50),
    spider VARCHAR(50),
    server VARCHAR(50),
    date TIMESTAMP
)