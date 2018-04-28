-- +migrate Up
alter table device_activation
    rename column nwk_s_key to f_nwk_s_int_key;

alter table device_activation
    add column s_nwk_s_int_key bytea,
    add column nwk_s_enc_key bytea;

update device_activation
set
    s_nwk_s_int_key = f_nwk_s_int_key,
    nwk_s_enc_key = f_nwk_s_int_key;

alter table device_activation
    alter column s_nwk_s_int_key set not null,
    alter column nwk_s_enc_key set not null;

-- +migrate Down
alter table device_activation
    drop column s_nwk_s_int_key,
    drop column nwk_s_enc_key;

alter table device_activation
    rename column f_nwk_s_int_key to nwk_s_key;
