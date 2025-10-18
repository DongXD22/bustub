-- 对于所有举办过 Athletics 学科竞赛的场馆，
-- 列出在这些场馆参赛的所有运动员，
-- 并按其国籍国家到所代表国家的距离降序排列，然后按姓名字母顺序排列。
-- 详情：运动员可以参加任何项目，
-- 并在这些场馆中以 a team member or an individual 的身份参赛。
-- 两个国家之间的距离计算为其纬度和经度差的平方和。
-- 仅输出具有 valid 信息的运动员。
-- （即运动员出现在运动员表中，并且两个国家都有非空的纬度和经度。）
-- 假设如果一支队伍参加比赛， all 队员正在参赛。
-- ATHLETE_NAME|REPRESENTED_COUNTRY_CODE|NATIONALITY_COUNTRY_CODE
-- GREEN Joseph|GUM|USA
with V_Athletics as(
    SELECT * 
    from venues
    WHERE disciplines like "%Athletics%"
)
,A_code as(
    select participant_code
    from V_Athletics
    inner join results
    on V_Athletics.venue=results.venue
)
,A_p_p as(
    SELECT name,country_code,nationality_code
    from athletes
    INNER JOIN A_code
    on athletes.code=A_code.participant_code
)
,A_t_p_code as(
    SELECT athletes_code
    from teams
    INNER JOIN A_code
    on A_code.participant_code=teams.code
)
,A_t_p as(
    SELECT name,country_code,nationality_code
    from athletes
    INNER JOIN A_t_p_code
    on athletes.code=A_t_p_code.athletes_code
)
,A_p as(
    SELECT * from A_t_p
    UNION
    SELECT * from A_p_p
)
,A as(
    select name,
        c,
        n,
        c_la,
        c_lo,
        Latitude as n_la,
        Longitude as n_lo
    from
        (select name,
            country_code as c,
            nationality_code as n,
            Latitude as c_la,
            Longitude as c_lo
        from A_p
        inner join countries
        on  A_p.country_code=countries.code
        WHERE countries.Latitude IS NOT NULL) as temp
    INNER JOIN countries
    on temp.n=countries.code
    WHERE countries.Latitude IS NOT NULL
)
select 
    name as ATHLETE_NAME,
    c as REPRESENTED_COUNTRY_CODE,
    n as NATIONALITY_COUNTRY_CODE,
    (c_la-n_la)*(c_la-n_la)
    +(c_lo-n_lo)*(c_lo-n_lo) as dist
from A
order by 
    (c_la-n_la)*(c_la-n_la)
    +(c_lo-n_lo)*(c_lo-n_lo) DESC,
    name ASC;
