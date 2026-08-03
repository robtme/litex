CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL REFERENCES parent (id) ON DELETE CASCADE
);

INSERT INTO parent (id, name) VALUES (1, 'original');
INSERT INTO child (id, parent_id) VALUES (1, 1);

-- Rebuild the parent table (e.g. to add a column). With foreign key enforcement enabled,
-- DROP TABLE parent performs an implicit cascading delete that wipes the child row. Migrate
-- disables enforcement for the duration of the migration so the data survives.
CREATE TABLE parent_new (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT);
INSERT INTO parent_new (id, name) SELECT id, name FROM parent;
DROP TABLE parent;
ALTER TABLE parent_new RENAME TO parent;
