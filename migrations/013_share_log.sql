-- Log global ordonne des shares, alimente par le worker share_shipper.
--
-- Le seq est gapless et suit l'ordre de commit: chaque batch prend le row lock
-- de share_seq, ce qui serialise les shippers de toutes les instances. Un
-- bigserial ne le garantirait pas (ordre des ids != ordre de commit).
--
-- epoch: incrementee A LA MAIN apres un failover Postgres asynchrone, qui peut
-- perdre les derniers batchs et faire rejouer des seq deja distribues. Le
-- consommateur suit (epoch, seq), pas seq seul.
--   UPDATE share_seq SET epoch = epoch + 1 WHERE id = 1;
CREATE TABLE share_seq
(
    id    int PRIMARY KEY CHECK (id = 1),
    epoch bigint NOT NULL,
    v     bigint NOT NULL
);

INSERT INTO share_seq (id, epoch, v)
VALUES (1, 1, 0);

-- Tout share d'une instance avec created_at <= shipped_until est deja dans share_log.
CREATE TABLE instance_watermark
(
    pool_instance text PRIMARY KEY,
    shipped_until timestamptz NOT NULL,
    heartbeat_at  timestamptz NOT NULL
);

CREATE TABLE share_log
(
    epoch         bigint      NOT NULL,
    seq           bigint      NOT NULL,
    created_at    timestamptz NOT NULL,
    pool_instance text        NOT NULL,
    path          text        NOT NULL,
    byte_offset   bigint      NOT NULL,
    workinfoid    bigint,
    username      text,
    workername    text,
    diff          double precision,
    sdiff         double precision,
    result        boolean,
    parse_error   boolean     NOT NULL DEFAULT false,
    raw           text,
    PRIMARY KEY (epoch, seq, created_at),
    UNIQUE (pool_instance, path, byte_offset, created_at)
);

COMMENT ON COLUMN share_log.created_at IS 'ckpool createdate, microsecond precision (nanoseconds truncated); fallback timestamp for parse_error rows';
COMMENT ON COLUMN share_log.path IS '<block>/<file>.sharelog relative to the ckpool log dir; with pool_instance and byte_offset it is the deterministic share id';
COMMENT ON COLUMN share_log.raw IS 'raw line, only when parse_error';

-- Comme pour share_weights, TimescaleDB impose created_at dans toute contrainte
-- unique; sans consequence, il est deterministe pour un share donne.
SELECT create_hypertable('share_log', by_range('created_at', INTERVAL '1 day'));
CREATE INDEX ON share_log (epoch, seq);

ALTER TABLE share_log SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'pool_instance',
  timescaledb.compress_orderby   = 'epoch, seq, created_at'
);

SELECT add_compression_policy('share_log', INTERVAL '2 days');
SELECT add_retention_policy('share_log', INTERVAL '30 days');
