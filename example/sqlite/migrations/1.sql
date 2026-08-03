CREATE TABLE users
(
    id      TEXT PRIMARY KEY,
    created TIMESTAMP NOT NULL,
    updated TIMESTAMP NOT NULL,
    email   TEXT      NOT NULL UNIQUE,
    name    TEXT      NOT NULL DEFAULT '',
    role    TEXT      NOT NULL DEFAULT 'member'
);
