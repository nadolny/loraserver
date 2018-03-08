-- +migrate Up
create table gateway_configuration (
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,

    id uuid primary key,
    channels smallint[] not null
);

create index idx_gateway_configuration_created_at on gateway_configuration(created_at);
create index idx_gateway_configuration_updated_at on gateway_configuration(updated_at);

create table gateway_configuration_extra_channel (
    id bigserial primary key,
    gateway_configuration_id uuid not null references gateway_configuration on delete cascade,
    modulation varchar(10) not null,
    frequency integer not null,
    bandwidth integer not null,
    bitrate integer not null,
    spreading_factors smallint[]
);

create index idx_gateway_configuration_extra_channel_gw_configuration_id on gateway_configuration_extra_channel(gateway_configuration_id);

-- +migrate Down
drop index idx_gateway_configuration_extra_channel_gw_configuration_id;
drop table gateway_configuration_extra_channel;

drop index idx_gateway_configuration_updated_at;
drop index idx_gateway_configuration_created_at;
drop table gateway_configuration;
