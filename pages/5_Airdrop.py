import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="🤑 Optimism token (OP) airdrop",
    layout= "wide",
    page_icon="🤑 Optimism token (OP) airdrop",
)
st.title("🤑 Optimism token (OP) airdrop")
st.sidebar.success("🤑 Optimism token (OP) airdrop")

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
#total airdrop
df1_query="""
select 
count (distinct event_inputs:recipient) as claimers,
sum(event_inputs:amount/1e18) as claimed_volume,
avg(event_inputs:amount/1e18) as avg_claimed_volume,
min (event_inputs:amount/1e18) as min_claimed_volume,
max (event_inputs:amount/1e18) as max_claimed_volume,
214748364.80::numeric as tot_vol,
248699::numeric as tot_user,
(claimers/tot_user)*100 as claimers_ratio,
(claimed_volume/tot_vol)*100 as claimed_ratio,
tot_user - claimers as remaining_users,
tot_vol - claimed_volume as remaining_volume
from optimism.core.fact_event_logs
where origin_to_address = '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
and origin_function_signature = '0x2e7ba6ef'
and event_name = 'Claimed'
"""
df1 = querying_pagination(df1_query)


#daily airdrop
df2_query="""
with price as (
select
hour::date as date,
avg(price) as price
from optimism.core.fact_hourly_token_prices
where token_address = '0x4200000000000000000000000000000000000042'
group by 1
)
select 
block_timestamp::date as date,
count (distinct event_inputs:recipient) as claimers,
sum(event_inputs:amount/1e18) as claimed_volume,
sum((claimers/248699)*100) over (order by date) as claimers_ratio,
sum((claimed_volume/214748364.80)*100) over (order by date) as claimed_ratio
from optimism.core.fact_event_logs 
where origin_to_address = '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
and origin_function_signature = '0x2e7ba6ef'
and event_name = 'Claimed'
group by 1
"""
df2 = querying_pagination(df2_query)

#holders
df3_query="""
with prices as (
select
avg(price) as price
from optimism.core.fact_hourly_token_prices
where token_address = '0x4200000000000000000000000000000000000042'
),transfer as (SELECT
FROM_ADDRESS as wallet,
((RAW_AMOUNT / 1e18) * -1) as volume
FROM optimism.core.fact_token_transfers
where CONTRACT_ADDRESS = '0x4200000000000000000000000000000000000042'
union
select
TO_ADDRESS as wallet,
(RAW_AMOUNT / 1e18) as volume
FROM optimism.core.fact_token_transfers
where CONTRACT_ADDRESS = '0x4200000000000000000000000000000000000042'
    ), main as(select 
wallet,
sum(volume) as balance
from transfer
group by 1
  )
select 
case
when balance BETWEEN 0 and 10 then 'Shrimp 0-10 OP' 
when balance  BETWEEN 10 and 100 then 'Crab 10-100 OP'  
when balance  BETWEEN 100 and 500 then 'Octopus 100-500 OP'  
when balance BETWEEN 500 and 1000 then 'Fish 500-1k OP'  
when balance BETWEEN 1000 and 10000 then 'Dolphin 1k-10k OP'  
when balance BETWEEN 10000 and 100000 then 'Shark 10k-100k OP'  
else 'Whale >100k USD' end as tier,
count(distinct wallet) as count_user
from main
where tier is not null
group by 1
"""
df3 = querying_pagination(df3_query)

st.subheader('Overal detail about optimism airdrop and there claimer')
cc1, cc2,cc3= st.columns([1,1,1])
 
with cc1:
    st.metric(
 value="{0:,.0f}".format(df1["claimers"][0]),
 label="Total number of claimers",
)
with cc2:
    st.metric(
 value="{0:,.0f}".format(df1["tot_vol"][0]),
 label="Total volume of Optimism token airdrop",
)
with cc3:
    st.metric(
 value="{0:,.0f}".format(df1["tot_user"][0]),
 label="Total number of eligible user",
)
cc1, cc2,cc3,cc4= st.columns([1,1,1,1])
 
with cc1:
    st.metric(
 value="{0:,.0f}".format(df1["claimed_volume"][0]),
 label="Total volume of claimed token",
)
with cc2:
    st.metric(
 value="{0:,.0f}".format(df1["avg_claimed_volume"][0]),
 label="Average volume of claimed token per user",
)
with cc3:
    st.metric(
 value="{0:,.0f}".format(df1["min_claimed_volume"][0]),
 label="Minimum volume of claimed token",
)
with cc4:
    st.metric(
 value="{0:,.0f}".format(df1["max_claimed_volume"][0]),
 label="Maximum volume of claimed token",
)
cc1, cc2,cc3,cc4= st.columns([1,1,1,1])
 
with cc1:
    st.metric(
 value="{0:,.0f}".format(df1["claimers_ratio"][0]),
 label="Claimed ratio from all tokens",
)
with cc2:
    st.metric(
 value="{0:,.0f}".format(df1["claimed_ratio"][0]),
 label="Claimer ratio from all eligible user",
)
with cc3:
    st.metric(
 value="{0:,.0f}".format(df1["remaining_users"][0]),
 label="Remaining users than did'nt claimed there tokens",
)
with cc4:
    st.metric(
 value="{0:,.0f}".format(df1["remaining_volume"][0]),
 label="Remaining token's volume than did'nt claimed yet",
)
st.subheader('Daily Optimism airdrop calim count')
cc1, cc2= st.columns([1, 1])
 
with cc1:
 st.caption('Daily Optimism airdrop calimer count')
 st.bar_chart(df2, x='date', y = 'claimers', width = 400, height = 400)
with cc2:
 st.caption('Daily cumulative Optimism airdrop calimer count')
 st.line_chart(df2, x='date', y = 'claimers_ratio', width = 400, height = 400)

st.subheader('Daily Optimism airdrop calimed volume')
cc1, cc2= st.columns([1, 1])
 
with cc1:
 st.caption('Daily Optimism airdrop calim volume')
 st.bar_chart(df2, x='date', y = 'claimed_volume', width = 400, height = 400)
with cc2:
 st.caption('Daily cumulative Optimism airdrop calimed volume')
 st.line_chart(df2, x='date', y = 'claimed_ratio', width = 400, height = 400)

st.subheader('Optimism Token (OP) Holders categorize')

st.caption('Optimism Token (OP) Holders categorize')
st.bar_chart(df3, x='tier', y = 'count_user', width = 400, height = 400)
