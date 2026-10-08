delete
from users;
ALTER TABLE users
    DROP CONSTRAINT users_pk;
ALTER TABLE users
    ADD CONSTRAINT users_pk PRIMARY KEY (address, pool_instance);

alter table users
    add column pool_instance text not null