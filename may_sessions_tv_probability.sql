select 
    c.*,
    t.tv_probability,
    t.tvp_id,
    t.spot_weight,
    if(
        channel IN ('Bing CPC Brand','Organic Search Brand','Google CPC Brand','Google CPC Non Brand','Organic Search Non Brand','PLA','PSM','Apple Search Ads','Bing CPC Non Brand', 'Direct App','Direct'),
        True,
        False
    ) as tv_relevant_session,
    if(
        tv_probability is not null,
        True,
        False
    ) as tv_sessions
from `tripis-332521.adhoc_data.clean_may_sessions` c
left join `tripis-332521.adhoc_data.tv_probability_by_time_` t on t.timestamp = c.timestamp