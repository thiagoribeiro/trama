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
        if exists (select 1 from pg_class where relname = 'saga_step_call' and relkind = 'p') then
            execute format(
                'create table if not exists saga_step_call_%s partition of saga_step_call for values from (%L) to (%L)',
                partition_name,
                current_month::timestamptz,
                (current_month + interval '1 month')::timestamptz
            );
        end if;
        current_month := current_month + interval '1 month';
    end loop;
end $$;
