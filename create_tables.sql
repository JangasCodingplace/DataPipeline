CREATE TABLE IF NOT EXISTS public.equipments (
    equipment_id varchar(10) PRIMARY KEY,
    amount float NOT NULL,
    contract_start_date date,
    construction_year smallint
)

CREATE TABLE IF NOT EXISTS public.notifications (
    notification_id bigint PRIMARY KEY,
    priority float,
    failure_start_date date NOT NULL,
    failure_start_time time NOT NULL,
    location varchar(4) NOT NULL,
    equipment_id varchar(10) NOT NULL
)

CREATE TABLE IF NOT EXISTS public.locations (
    location varchar(4),
    failure_start_date date NOT NULL,
    notification_count int NOT NULL,
    PRIMARY KEY (location, failure_start_date)
)

CREATE TABLE IF NOT EXISTS public.equipment_notifications (
    equipment_id varchar(10) PRIMARY KEY,
    amount float NOT NULL,
    notification_count int NOT NULL,
    PRIMARY KEY(equipment_id)
)
