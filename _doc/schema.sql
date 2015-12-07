-- For use with PostgreSQL and PowerDNS.

-- This is the schema specific to namesync. It requires and builds on the
-- PowerDNS PostgreSQL schema. You must have already loaded the PowerDNS
-- schema. If you edited the PowerDNS schema, you may also have to edit this
-- schema.
--
-- Be warned that this schema extends the PowerDNS schema.

-- Table for tracking current synchronization state.
-- Never contains more than one row.
CREATE TABLE namecoin_state (
  cur_block_hash char(64),
  cur_block_height integer
);

-- Table for grouping records by Namecoin name.
CREATE TABLE namecoin_domains (
  id serial NOT NULL,
  last_height integer,
  name varchar(255)
);

ALTER TABLE namecoin_domains
  ADD CONSTRAINT pk_namecoin_domain
    PRIMARY KEY (id);

CREATE UNIQUE INDEX idx_namecoin_domains__name
  ON namecoin_domains (name);

-- Add the reference to namecoin_domains to the records table.
ALTER TABLE records
  ADD COLUMN namecoin_domain_id integer;

CREATE INDEX fki_records__namecoin_domain_id ON records (namecoin_domain_id);

ALTER TABLE records
  ADD CONSTRAINT fk_records__namecoin_domain_id
    FOREIGN KEY (namecoin_domain_id)
      REFERENCES namecoin_domains(id)
      ON DELETE CASCADE;

-- Table of the last X blocks.
CREATE TABLE namecoin_prevblock (
  id serial NOT NULL,
  block_hash char(64),
  block_height integer
);

ALTER TABLE namecoin_prevblock
  ADD CONSTRAINT pk_namecoin_prevblock
    PRIMARY KEY(id);

-- import/delegate dependency tracking.
CREATE TABLE namecoin_deps (
  namecoin_domain_id integer NOT NULL,
  name varchar(255) NOT NULL,
  deferred boolean NOT NULL DEFAULT false,
  CONSTRAINT pk_namecoin_deps PRIMARY KEY (namecoin_domain_id, name),
  CONSTRAINT fk_namecoin_deps__namecoin_domain_id FOREIGN KEY (namecoin_domain_id)
  REFERENCES namecoin_domains (id) ON DELETE CASCADE
);
