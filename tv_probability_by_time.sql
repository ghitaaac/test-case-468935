with 

    tv_planning as (
        select
            c.*,
            timestamp_trunc(timestamp_add(block_start_time, interval block_position second), minute) as real_start_date,
            timestamp_add(timestamp_trunc(timestamp_add(block_start_time, interval block_position second), minute), interval 5 minute) as real_end_date,
            t.duration as spot_duration,
            t.spot_name,
            au.reach as tv_show_reach
        from `tripis-332521.adhoc_data.clean_tv_planning` c
        left join `tripis-332521.adhoc_data.tv_spots` t on t.spot_id = c.spot_id
        left join `tripis-332521.adhoc_data.audience` au on au.tv_show = c.tv_show
    ),

    tv_sessions_att as (
        select
            *,
            last_value(spot_name ignore nulls) over(partition by aux order by timestamp asc rows between 5 preceding and current row ) as aux_spot_name,
            last_value(tv_show_reach ignore nulls) over(partition by aux order by timestamp asc rows between 5 preceding and current row) as aux_tv_show_reach,
            last_value(tvp_id ignore nulls) over(partition by aux order by timestamp asc rows between 5 preceding and current row) as tvp_id_
        from `tripis-332521.adhoc_data.raw_tv_sessions_info` r
        left join tv_planning t on t.real_start_date = r.timestamp
    ),

    tv_sessions_planning as (
        select 
            * except(tvp_id),
            tvp_id_ as tvp_id,
            if(
                aux_spot_name is not null and visits > peak_threshold,
                ((visits - background_noise)/visits) * ((visits/aux_tv_show_reach)*100),
                null
            ) as tv_probability,
            ((visits/aux_tv_show_reach)*100) as spot_weight
        from tv_sessions_att
    )



select * from tv_sessions_planning