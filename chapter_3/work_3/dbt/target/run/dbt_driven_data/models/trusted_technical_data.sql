
  
    

  create  table "airflow"."airflow_trusted"."technical_data__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT
        fnu.unique_id,
        da.address,
        da.mac_address,
        da.ip_address,
        CAST(fnu.download_speed AS numeric) AS download_speed,
        CAST(fnu.upload_speed AS numeric) AS upload_speed,
        ROUND((CAST(fnu.session_duration AS numeric) / 60), 1) AS min_session_duration,
        CASE 
            WHEN CAST(fnu.download_speed AS numeric) < 50 
                 OR CAST(fnu.upload_speed AS numeric) < 30 
                 OR CAST(fnu.session_duration AS numeric) / 60 < 1 THEN true
            ELSE false
        END AS technical_issue
    FROM
        "airflow"."airflow_staging"."fact_network_usage" fnu
    JOIN
        "airflow"."airflow_staging"."dim_address" da
    ON
        fnu.unique_id = da.unique_id
)

SELECT
    *
FROM
    source_data
  );
  