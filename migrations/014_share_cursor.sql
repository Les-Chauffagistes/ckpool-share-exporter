-- Curseurs du share_shipper, independants de la table file: ni la decouverte de
-- file_explorer ni la retention de file ne les concernent.
--
-- path est relatif au repertoire de logs (<block>/<file>.sharelog), donc stable
-- d'un montage a l'autre.
CREATE TABLE share_cursor
(
    pool_instance  text        NOT NULL,
    path           text        NOT NULL,
    block          text        NOT NULL,
    shipped_offset bigint      NOT NULL DEFAULT 0,
    closed         boolean     NOT NULL DEFAULT false,
    blocked        boolean     NOT NULL DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_instance, path)
);

COMMENT ON COLUMN share_cursor.closed IS 'block over, file read to the end';
COMMENT ON COLUMN share_cursor.blocked IS 'file shrank: shipping stopped, manual review needed';

CREATE INDEX ON share_cursor (pool_instance) WHERE NOT closed;

-- Seuls les curseurs fermes sont purges: un curseur ouvert n'est jamais supprime
-- sous les pieds du shipper. Meme mecanique que 006.
CREATE OR REPLACE PROCEDURE delete_expired_share_cursor_rows(job_id integer, config jsonb)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM share_cursor
    WHERE closed
      AND updated_at < now() - INTERVAL '30 days';
END;
$$;

SELECT add_job('delete_expired_share_cursor_rows', INTERVAL '1 day');
