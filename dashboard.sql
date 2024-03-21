with tab as (with sl as (
    select
        s.visitor_id,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        date(s.visit_date) as visit_date,
        row_number()
            over (partition by s.visitor_id order by s.visit_date desc)
        as rn
    from sessions as s
    left join leads as l
        on
            s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
    where
        s.medium in (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp',
            'tg', 'social'
        )
),

sl1 as (
    select
        *,
        case
            when sl.lead_id is not null then 1  -- noqa: RF03
            else 0
        end as is_lead,
        case
            when sl.status_id = 142 then 1
            else 0
        end as success_purchase
    from sl
    where sl.rn = 1
),

ads as (
    select
        date(vk_ads.campaign_date) as campaign_date,
        vk_ads.utm_campaign,
        vk_ads.utm_medium,
        vk_ads.utm_source,
        vk_ads.daily_spent
    from vk_ads

    union all

    select
        date(ya_ads.campaign_date) as campaign_date,
        ya_ads.utm_campaign,
        ya_ads.utm_medium,
        ya_ads.utm_source,
        ya_ads.daily_spent
    from ya_ads
),

aggregated_ads as (
    select
        campaign_date,
        utm_campaign,
        utm_medium,
        utm_source,
        sum(daily_spent) as total_date_cost
    from ads
    group by
        campaign_date,
        utm_campaign,
        utm_medium,
        utm_source
)

select
    sl1.visit_date,
    sl1.utm_source,
    sl1.utm_medium,
    sl1.utm_campaign,
    count(sl1.visitor_id) as visitors_count,
    round(avg(ads.total_date_cost)) as total_cost,
    sum(sl1.is_lead) as leads_count,
    sum(sl1.success_purchase) as purchases_count,
    sum(sl1.amount) as revenue
from sl1
left join aggregated_ads as ads
    on
        sl1.visit_date = ads.campaign_date
        and
        sl1.utm_campaign = ads.utm_campaign
        and
        sl1.utm_medium = ads.utm_medium
        and
        sl1.utm_source = ads.utm_source  
group by sl1.visit_date, sl1.utm_source, sl1.utm_medium, sl1.utm_campaign
order by
    revenue desc nulls last,
    sl1.visit_date asc,
    visitors_count asc,
    sl1.utm_source asc,
    sl1.utm_medium asc,
    sl1.utm_campaign asc)
    
-- 1.Сколько у нас пользователей заходят на сайт?
    
--select sum(visitors_count) from tab;
    
--2.Сколько лидов к нам приходят?

--select sum(leads_count) from tab;
    
-- 3.Какие каналы их приводят на сайт?

--select 
--    utm_source,
--    sum(visitors_count) as visitors_count,
--    sum(leads_count) as leads_count
--    from tab
--    group by utm_source
--    order by visitors_count desc;

-- 4.Какая конверсия из клика в лид? Из лидов в успешные сделки?
    
--select 
--    utm_source,
--    round
--        (sum(leads_count) /
--         sum(visitors_count) * 100, 2) as visitors_to_leads,
--    case
--        when sum(coalesce(leads_count, 0)) = 0 then 0
--        else
--            round ( sum(coalesce(purchases_count, 0)) /
--                sum(leads_count)
--                * 100,
--                2) end as leads_to_purchase
--    
--    from tab
--    group by utm_source
--    order by visitors_to_leads desc;
    
--5. Сколько мы тратим по разным каналам в динамике?
    
--    select
--    case
--        when extract(day from visit_date) between 1 and 7 then 1
--        when extract(day from visit_date) between 8 and 14 then 2
--        when extract(day from visit_date) between 15 and 21 then 3
--        else 4
--    end as week,
--    utm_source,
--    sum(total_cost)
--    
--from tab
--group by 1, 2
--having sum(total_cost) > 0;
    
--6.Окупаются ли каналы?
--select
--    utm_source,
--    sum(total_cost) as total_cost,
--    sum(revenue) as revenue
--from tab
--group by 1
--having sum(total_cost)>0 or sum(revenue)>0;

    





    

