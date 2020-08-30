from airflow.hooks.postgres_hook import PostgresHook


def create_location_table():
    redshift_hook = PostgresHook('redshift')
    sql = """
        SELECT
            location,
            failure_start_date,
            COUNT(location) AS "notification_count"
        INTO TABLE locations
        FROM notifications
        GROUP BY (location, failure_start_date);
    """

    redshift_hook.get_records(sql)


def create_equipment_notification_table():
    redshift_hook = PostgresHook('redshift')
    sql = """
        SELECT
            n.equipment_id,
            e.amount,
            COUNT(n.equipment_id) AS "notification_count"
        INTO TABLE equipment_notifications
        FROM notifications AS n
        RIGHT JOIN equipments AS e
        ON n.equipment_id = e.equipment_id
        GROUP BY (n.equipment_id, e.amount)
    """
    redshift_hook.get_records(sql)
