import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="🌉 Bridge Activity",
    layout= "wide",
    page_icon="🌉",
)
st.title("🌉 Bridge Activity")
st.sidebar.success("🌉 Bridge Activity")

@st.cache(ttl=10000)
def querying_pagination(query_string):
    sdk = ShroomDK('8c37dc3a-fcf4-42a1-a860-337fa9931a2a')
    result_list = []
    for i in range(1,11): 
        data=sdk.query(query_string,page_size=100000,page_number=i)
        if data.run_stats.record_count == 0:  
            break
        else:
            result_list.append(data.records)
  
    result_df=pd.DataFrame()
    for idx, each_list in enumerate(result_list):
        if idx == 0:
            result_df=pd.json_normalize(each_list)
        else:
            result_df=pd.concat([result_df, pd.json_normalize(each_list)])

    return result_df
#daily bridge
df_query="""
with standard as (select
date_trunc('week',block_timestamp) as date,
'ETH' as symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and symbol = 'WETH'
group by 1,2
union 
select
date_trunc('week',block_timestamp) as date,
a.symbol,
count (distinct tx_hash) as tx_count,
count (DISTINCT(case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and contract_address = token_address
group by 1,2
),hop as (select
date_trunc('week',block_timestamp) as date,
'ETH' as symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and symbol = 'WETH'
group by 1,2
union 
select
date_trunc('week',block_timestamp) as date,
a.symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and contract_address = token_address
group by 1,2
),across as (select
date_trunc('week',block_timestamp) as date,
'ETH' as symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and symbol = 'WETH'
group by 1,2
union 
select
date_trunc('week',block_timestamp) as date,
a.symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = token_address
group by 1,2
)
select 
date::date as date,
'Standard Op' as bridge,
tx_count,
count_user,
volume_usd
from standard 
union 
select 
date::date as date,
'Across' as bridge,
tx_count,
count_user,
volume_usd
from across
union 
select 
date::date as date,
'Hop' as bridge,
tx_count,
count_user,
volume_usd
from hop
"""
df = querying_pagination(df_query)


#total bridge
df1_query="""
with standard as (select
block_timestamp::date as date,
'ETH' as symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address != '0x25ace71c97b33cc4729cf772ae268934f7ab5fa1' then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x25ace71c97b33cc4729cf772ae268934f7ab5fa1'
and symbol = 'WETH'
group by 1,2
union 
select
block_timestamp::date as date,
a.symbol,
count (distinct tx_hash) as tx_count,
count (DISTINCT(case when origin_from_address != '0x25ace71c97b33cc4729cf772ae268934f7ab5fa1' then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x25ace71c97b33cc4729cf772ae268934f7ab5fa1'
and contract_address = token_address
group by 1,2
),hop as (select
block_timestamp::date as date,
'ETH' as symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and symbol = 'WETH'
group by 1,2
union 
select
block_timestamp::date as date,
a.symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and contract_address = token_address
group by 1,2
),across as (select
block_timestamp::date as date,
'ETH' as symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and origin_function_signature in ('0x49228978')
and symbol = 'WETH'
group by 1,2
union 
select
block_timestamp::date as date,
a.symbol,
count (distinct tx_hash) as tx_count,
count(DISTINCT(case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end)) as count_user,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = token_address
and origin_function_signature in ('0x49228978')
group by 1,2
)
select 
'Standard Op' as bridge,
tx_count,
count_user,
volume_usd
from standard 
union 
select 
'Across' as bridge,
tx_count,
count_user,
volume_usd
from across
union 
select 
'Hop' as bridge,
tx_count,
count_user,
volume_usd
from hop
"""
df1 = querying_pagination(df1_query)

#user categorize by count of bridge
df2_query="""
with standard as (select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and contract_address = token_address
group by 1
),hop as (select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and contract_address = token_address
group by 1
),across as (select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = token_address
group by 1
), main as
(select 
user,
'Standard Op' as bridge,
tx_count,
volume_usd
from standard 
union 
select 
user,
'Across' as bridge,
tx_count,
volume_usd
from across
union 
select 
user,
'Hop' as bridge,
tx_count,
volume_usd
from hop)
select 
case
when tx_count = 1 then 'bridge just once time' 
when tx_count  BETWEEN 2 and 5 then 'bridge 2 - 5 time'  
when tx_count  BETWEEN 6 and 15 then 'bridge 5 - 15 time'  
when tx_count BETWEEN 16 and 50 then 'bridge 15 - 50 time'  
when tx_count BETWEEN 51 and 100 then 'bridge 50 - 100 time'  
when tx_count BETWEEN 101 and 200 then 'bridge 100 - 200 time'  
else 'bridge more than 200 time' end as tier,
bridge,
count(distinct user) as count_bridger
from main
where tier is not null
group by 1,2
"""
df2 = querying_pagination(df2_query)

#user categorize by volume (USD) of bridge
df3_query="""
with standard as (select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and contract_address = token_address
group by 1
),hop as (select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and contract_address = token_address
group by 1
),across as (select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = token_address
group by 1
), main as
(select 
user,
'Standard Op' as bridge,
tx_count,
volume_usd
from standard 
union 
select 
user,
'Across' as bridge,
tx_count,
volume_usd
from across
union 
select 
user,
'Hop' as bridge,
tx_count,
volume_usd
from hop)
select 
case
when volume_usd BETWEEN 0 and 10 then 'Shrimp 0-10 USD' 
when volume_usd  BETWEEN 10 and 100 then 'Crab 10-100 USD'  
when volume_usd  BETWEEN 100 and 500 then 'Octopus 100-500 USD'  
when volume_usd BETWEEN 500 and 1000 then 'Fish 500-1k USD'  
when volume_usd BETWEEN 1000 and 10000 then 'Dolphin 1k-10k USD'  
when volume_usd BETWEEN 10000 and 100000 then 'Shark 10k-100k USD'  
else 'Whale >100k USD' end as tier,
bridge,
count(distinct user) as count_bridger
from main
where tier is not null
group by 1,2
"""
df3 = querying_pagination(df3_query)

#top 10 user by count of bridge
df4_query="""
with standard as (select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and contract_address = token_address
group by 1
),hop as (select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and contract_address = token_address
group by 1
),across as (select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = token_address
group by 1
), main as
(select 
user,
'Standard Op' as bridge,
tx_count,
volume_usd
from standard 
union 
select 
user,
'Across' as bridge,
tx_count,
volume_usd
from across
union 
select 
user,
'Hop' as bridge,
tx_count,
volume_usd
from hop)
select 
*
from main
order by 3 desc 
limit 10
"""
df4 = querying_pagination(df4_query)

#top 10 user by volume of bridge
df5_query="""
with standard as (select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1' then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
and contract_address = token_address
group by 1
),hop as (select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2')) then origin_to_address else origin_from_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address in (lower('0xb8901acb165ed027e32754e0ffe830802919727f'),lower('0x3666f603cc164936c1b87e207f36beba4ac5f18a'),
lower('0x3e4a3a4796d16c0cd582c382691998f7c06420b6'),
lower('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'), lower('0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'))
and contract_address = token_address
group by 1
),across as (select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_eth_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and symbol = 'WETH'
group by 1
union 
select
DISTINCT (case when origin_from_address != lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381') then origin_from_address else origin_to_address end) as user,
count (distinct tx_hash) as tx_count,
sum (amount*price) as volume_usd
from ethereum.core.ez_token_transfers a join ethereum.core.fact_hourly_token_prices b on b.hour::date = a.block_timestamp::date
where origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = token_address
group by 1
), main as
(select 
user,
'Standard Op' as bridge,
tx_count,
volume_usd
from standard 
union 
select 
user,
'Across' as bridge,
tx_count,
volume_usd
from across
union 
select 
user,
'Hop' as bridge,
tx_count,
volume_usd
from hop)
select 
*
from main
where volume_usd is not null
order by 4 desc 
limit 10
"""
df5 = querying_pagination(df5_query)

options = st.multiselect(
    '**Select your desired bridge:**',
    options=df['bridge'].unique(),
    default=df['bridge'].unique(),
    key='brige'
)

if len(options) > 0:
 
 df = df.query("bridge == @options")
 df1 = df1.query("bridge == @options")
 df2 = df2.query("bridge == @options")
 df3 = df3.query("bridge == @options")
 df4 = df4.query("bridge == @options")
 df5 = df5.query("bridge == @options")
 st.write("""
 # Overal bridge activity #

 The Daily Number of bridgers , bridge count and bridge volume (USD) metrics is a measure of how many wallets / transaction with how much volume (USD) on optimisem are making bridge on chain.

 """
 )
 st.subheader('Total number of transaction at each bridge')
 cc1, cc2= st.columns([1, 1])
 
 with cc1:
  st.caption('Total and daily number of transaction at each bridge')
  st.bar_chart(df1, x='bridge', y = 'tx_count', width = 400, height = 400)
 with cc2:
  fig = px.pie(df1, values='tx_count', names='bridge', title='Total number of transaction at each bridge')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 fig = px.bar(df, x='date', y='tx_count',color='bridge', title='Daily number of transaction at each bridge')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 st.subheader('Total and daily number of user at each bridge')
 cc1, cc2= st.columns([1, 1])
 
 with cc1:
  st.caption('Total number of user at each bridge')
  st.bar_chart(df1, x='bridge', y = 'count_user', width = 400, height = 400)
 with cc2:
  fig = px.pie(df1, values='count_user', names='bridge', title='Total number of user at each bridge')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 fig = px.bar(df, x='date', y='count_user',color='bridge', title='Daily number of user at each bridge')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 st.subheader('Total and daily volume (usd) of transaction at each bridge')
 cc1, cc2= st.columns([1, 1])
 with cc1:
  st.caption('Total volume (usd) of transaction at each bridge')
  st.bar_chart(df1, x='bridge', y = 'volume_usd', width = 400, height = 400)
 with cc2:
  fig = px.pie(df1, values='volume_usd', names='bridge', title='Total volume (usd) of transaction at each bridge')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 fig = px.bar(df, x='date', y='volume_usd',color='bridge', title='Daily volume (usd) of transaction at each bridge')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 
 st.write("""
 # User categorize by count and volume (USD) of bridge #

 Here the bridgers are categorized based on the number and volume (USD) of the bridge.

 """
 )
 cc1, cc2 = st.columns([1, 1])

 with cc1:
  fig = px.bar(df2, x='tier', y='count_bridger',color='bridge', title='User categorize by count of bridge')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 with cc2:
  fig = px.pie(df2, values='count_bridger', names='tier', title='User categorize by rate of swap count')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme=None)

 cc1, cc2 = st.columns([1, 1])

 with cc1:
  fig = px.bar(df3, x='tier', y='count_bridger',color='bridge', title='User categorize by count of bridge')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 with cc2:
  fig = px.pie(df3, values='count_bridger', names='tier', title='User categorize by rate of bridge volume (USD)')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme=None)

 st.write("""
 # Top 10 bridger #

 Top users are based on the number and volume (USD) of the bridge are:

 """
 )
 cc1, cc2 = st.columns([1, 1])
 with cc1:
   st.caption('Top 10 bridger by count of bridge')
   st.bar_chart(df5, x='user', y = 'tx_count', width = 400, height = 400)
 with cc2:
   st.caption('Top 10 bridger by volume (USD) of bridge')
   st.bar_chart(df5, x='user', y = 'volume_usd', width = 400, height = 400)
