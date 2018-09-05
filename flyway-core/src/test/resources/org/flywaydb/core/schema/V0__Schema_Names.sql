CREATE TABLE schema_names (
  name TEXT NOT NULL
);

INSERT INTO schema_names (name)
VALUES (current_schema());