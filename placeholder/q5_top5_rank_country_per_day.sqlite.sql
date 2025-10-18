-- 对于每一天，找出在那个天的前五名（包含前五名）中出现次数最多的国家。
-- 对于这些国家，还要列出它们的人口排名和 GDP 排名。按日期升序排序输出。
-- 提示：使用 result 表，并使用 participant_code 获取相应的国家。
-- 如果你无法获取 result 表中某条记录的国家信息，忽略该记录。
-- 详细说明：在统计出现次数时，仅考虑 results 表中 rank 不为空的记录。
-- 从输出中排除所有 rank 值都为空的日期。如果出现次数相同，
-- 选择字母顺序靠前的国家。保持日期的原始格式。
-- 另外，在统计出现次数时，不要从 results 表中删除重复项。
-- （见 Important Clarifications 部分）。
-- DATE|COUNTRY_CODE|TOP5_APPEARANCES|GDP_RANK|POPULATION_RANK
-- 2024-07-25|KOR|7|38|22
with date_code as(
    select *
    from results
    where rank<=5
)
,a_ccode as (
    SELECT date,country_code,event_name,code
    from athletes
    INNER JOIN date_code
    on athletes.code=date_code.participant_code
)
,t_ccode as (
    SELECT distinct date,country_code,event_name,rank
    from teams 
    INNER JOIN date_code
    on teams.code=date_code.participant_code
)
,ccode as (
    SELECT date,country_code from t_ccode
    UNION all
    SELECT date,country_code from a_ccode
)
--select * from t_ccode ORDER BY country_code
,top5pday as(
    SELECT date,country_code,count(date) as a,rank(a)

    from ccode
    GROUP BY date,country_code
    ORDER BY adate,country_code,event_name,rank
)
SELECT *
    -- ,rank() over 
    -- (PARTITION by date order by a desc) as rk
from top5pday
where rk = 1
