CREATE TABLE package (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE statistic (
  package_id INTEGER NOT NULL REFERENCES package(id),
  day DATE NOT NULL,
  downloads INTEGER NOT NULL,

  UNIQUE (package_id, day)
);

CREATE VIEW package_statistic AS
  SELECT name, day, downloads
  FROM statistic
  INNER JOIN package ON package.id = statistic.package_id;
