with 
    tv_relevant_sessions as (
        SELECT
            *
        FROM `tripis-332521.adhoc_data.clean_may_sessions`
        WHERE channel IN ('Bing CPC Brand','Organic Search Brand','Google CPC Brand','Google CPC Non Brand','Organic Search Non Brand','PLA','PSM','Apple Search Ads','Bing CPC Non Brand', 'Direct App','Direct')
    ),

    agg_tv_sessions as (
        SELECT 
            'aux' as aux,
            timestamp,
            count(identifier) as visits
        FROM tv_relevant_sessions
        group by timestamp
        order by timestamp asc
    ),

    preceding_following_visits as (
        select
            aux,
            timestamp,
            visits,
            last_value(visits) over(partition by aux order by timestamp asc rows between unbounded preceding and 20 preceding ) as visits_x,
            first_value(visits) over(partition by aux order by timestamp asc rows between 20 following and unbounded following ) as visits_y
        from agg_tv_sessions
    ),

    raw_visits_info as (
        select 
            *,
            adhoc_data.median(visits_x, visits_y) as background_noise,
            adhoc_data.median(visits_x, visits_y) * 1.15 as peak_threshold
        from preceding_following_visits 
    )


select * from raw_visits_info