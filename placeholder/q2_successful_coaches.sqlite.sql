/*
 找出所有赢得过 at least one 枚奖牌的成功教练。
 按奖牌数量降序排列，
 然后按姓名字母顺序排列。
 
 详情：如果奖牌与教练共享 same country and discipline ，
 则该奖牌归教练所有，无论性别或项目如何。
 考虑使用 winner_code 枚奖牌来决定其所属国家。
 
 您的输出应如下所示：
 
 COACH_NAME|MEDAL_NUMBER
 您的第一行应如下所示：
 
 BRECKENRIDGE Grant|9 --
 */
with t as (
    select code,country_code from teams GROUP BY code,country_code
),
medal_coache as(
    
)
