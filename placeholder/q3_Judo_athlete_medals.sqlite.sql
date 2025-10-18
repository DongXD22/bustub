--找出所有在 Judo 项目中的运动员，并列出他们获得的奖牌数量。
--按奖牌数量降序排列，然后按姓名字母顺序排列。
--ATHLETE_NAME|MEDAL_NUMBER
with J_ath as(
    SELECT *
    from athletes
    where disciplines like '%Judo%'
)
,J_medal as(
    SELECT * 
    from medals 
    where discipline= "Judo"
)
,J_medal_t as (
    SELECT *
    from J_medal
    WHERE event like "Mixed Team"
)
,J_medal_p as(
    select J_ath.name as ATHLETE_NAME,count(J_medal.medal_code) as MEDAL_NUMBER
    from J_ath
    left join J_medal
    on J_ath.code=J_medal.winner_code
    GROUP BY J_ath.code
    ORDER BY MEDAL_NUMBER DESC,ATHLETE_NAME
)
,J_medal_t_code as(
    select athletes_code
    from teams
    inner join J_medal_t
    on teams.code=J_medal_t.winner_code
)
,J_medal_tt as(
    select name as ATHLETE_NAME,1 as MEDAL_NUMBER
    from athletes
    inner join J_medal_t_code
    on athletes.code=J_medal_t_code.athletes_code
)
,J_medalnum as(
    SELECT * from J_medal_tt
    UNION ALL
    select * from J_medal_p
)
SELECT ATHLETE_NAME,sum(MEDAL_NUMBER) as MEDAL_NUMBER 
from J_medalnum
GROUP BY ATHLETE_NAME
ORDER BY MEDAL_NUMBER DESC,ATHLETE_NAME ASC;