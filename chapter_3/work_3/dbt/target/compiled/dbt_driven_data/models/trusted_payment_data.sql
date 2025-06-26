

WITH source_data AS (
    SELECT
        fnu.unique_id,
        df.iban,
        CAST(fnu.download_speed AS numeric) AS download_speed,
        CAST(fnu.upload_speed AS numeric) AS upload_speed,
        CAST(fnu.session_duration AS numeric) AS session_duration,
        CAST(fnu.consumed_traffic AS numeric) AS consumed_traffic,
        (
            ((CAST(fnu.download_speed AS numeric) + CAST(fnu.upload_speed AS numeric) + 1) / 2) 
            + (CAST(fnu.consumed_traffic AS numeric) / (CAST(fnu.session_duration AS numeric) + 1))
        ) AS payment_amount
    FROM
        "airflow"."airflow_staging"."fact_network_usage" fnu
    JOIN
        "airflow"."airflow_staging"."dim_finance" df
    ON
        fnu.unique_id = df.unique_id
)

SELECT
    *
FROM
    source_data