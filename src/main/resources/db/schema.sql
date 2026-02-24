create table if not exists saga_execution (
    id uuid not null,
    name text not null,
    version text not null,
    definition jsonb,
    status text not null,
    failure_description text,
    callback_warning text,
    last_failed_step_index integer,
    last_failed_phase text,
    started_at timestamptz not null,
    completed_at timestamptz,
    updated_at timestamptz not null,
    primary key (id, started_at)
) partition by range (started_at);

create table if not exists saga_step_result (
    id bigserial not null,
    saga_id uuid not null,
    step_index integer not null,
    step_name text not null,
    phase text not null,
    status_code integer,
    success boolean not null,
    response_body jsonb,
    started_at timestamptz not null,
    created_at timestamptz not null,
    primary key (id, started_at)
) partition by range (started_at);

create index if not exists idx_saga_execution_started_at on saga_execution (started_at);
create index if not exists idx_saga_execution_status_started_at on saga_execution (status, started_at);

create index if not exists idx_saga_step_result_saga_id_started_at on saga_step_result (saga_id, started_at);
create index if not exists idx_saga_step_result_step_started_at on saga_step_result (saga_id, step_index, started_at, created_at desc);

create table if not exists saga_definition (
    id uuid primary key,
    name text not null,
    version text not null,
    definition jsonb not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create unique index if not exists idx_saga_definition_name_version on saga_definition (name, version);

do $$
declare
    start_month date := date_trunc('month', now())::date - interval '1 month';
    end_month date := date_trunc('month', now())::date + interval '13 months';
    current_month date;
    partition_name text;
begin
    current_month := start_month;
    while current_month < end_month loop
        partition_name := to_char(current_month, 'YYYYMM');
        execute format(
            'create table if not exists saga_execution_%s partition of saga_execution for values from (%L) to (%L)',
            partition_name,
            current_month::timestamptz,
            (current_month + interval '1 month')::timestamptz
        );
        execute format(
            'create table if not exists saga_step_result_%s partition of saga_step_result for values from (%L) to (%L)',
            partition_name,
            current_month::timestamptz,
            (current_month + interval '1 month')::timestamptz
        );
        current_month := current_month + interval '1 month';
    end loop;
end $$;
