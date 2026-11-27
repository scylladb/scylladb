CREATE TABLE bench.github_events (
  repo_id bigint,
  created_at timestamp,
  event_id bigint,
  event_type text,
  repo_name text,
  actor_id bigint,
  actor_login text,
  org_login text,
  is_public boolean,
  payload text,
  PRIMARY KEY ((repo_id), created_at, event_id)
);
