{{ config(materialized='table') }}

SELECT
    fact.event_id,
    fact.date_key,
    date.day,
    date.month,
    date.year,
    date.day_of_week,

    CASE
        WHEN EXTRACT(HOUR FROM fact.datetime_log::timestamp) BETWEEN 0 AND 4  THEN 'Late Night'
        WHEN EXTRACT(HOUR FROM fact.datetime_log::timestamp) BETWEEN 5 AND 7  THEN 'Early Morning'
        WHEN EXTRACT(HOUR FROM fact.datetime_log::timestamp) BETWEEN 8 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM fact.datetime_log::timestamp) BETWEEN 12 AND 13 THEN 'Lunch'
        WHEN EXTRACT(HOUR FROM fact.datetime_log::timestamp) BETWEEN 14 AND 18 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM fact.datetime_log::timestamp) BETWEEN 19 AND 21 THEN 'Prime Time'
        ELSE 'Night'
    END AS time_slot,

    fact.user_id,

    CASE
        WHEN fact.user_id = '00000000' THEN 'guest'

        WHEN EXISTS (
            SELECT 1
            FROM {{ source('gold', 'bridge_user_plan') }} b
            WHERE b.user_id = fact.user_id
            AND to_date(b.first_effective_date, 'YYYY-MM-DD')
                <= fact.datetime_log::date
            AND (
                    b.recent_effective_date IS NULL
                    OR to_date(b.recent_effective_date, 'YYYY-MM-DD')
                    >= fact.datetime_log::date
            )
        ) THEN 'plan'

        ELSE 'noplan'
    END AS user_state_at_search,


    platform.device_type,
    network.proxy_isp AS network_name,
    network.isp_group AS network_location,

    fact.keyword,
    main_cat.category_name AS main_category,
    sub1_cat.category_name AS sub1_category,
    sub2_cat.category_name AS sub2_category,
    sub3_cat.category_name AS sub3_category

FROM {{ source('gold', 'fact_customer_search') }} fact

JOIN {{ source('gold', 'dim_date') }} date
    ON fact.date_key = date.date_key

LEFT JOIN {{ source('gold', 'dim_platform') }} platform
    ON fact.platform_key = platform.platform_key

LEFT JOIN {{ source('gold', 'dim_network') }} network
    ON fact.network_key = network.network_key

LEFT JOIN {{ source('gold', 'dim_category') }} main_cat
    ON fact.main_keyword_category = main_cat.category_key

LEFT JOIN {{ source('gold', 'dim_category') }} sub1_cat
    ON fact.sub1_keyword_category = sub1_cat.category_key

LEFT JOIN {{ source('gold', 'dim_category') }} sub2_cat
    ON fact.sub2_keyword_category = sub2_cat.category_key

LEFT JOIN {{ source('gold', 'dim_category') }} sub3_cat
    ON fact.sub3_keyword_category = sub3_cat.category_key

WHERE fact.category = 'enter'