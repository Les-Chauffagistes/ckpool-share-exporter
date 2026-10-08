DELETE FROM users;
ALTER TABLE users
    DROP CONSTRAINT users_pk;
ALTER TABLE users
    ADD COLUMN pool_instance text NOT NULL;
ALTER TABLE users
    ADD CONSTRAINT users_pk PRIMARY KEY (address, pool_instance);
