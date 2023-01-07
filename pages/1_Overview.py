import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="📊 Overview",
    layout= "wide",
    page_icon="📊",
)
st.title("📊 Overview")
st.sidebar.success("📊 Overview")


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
#daily tx
df_query="""
with price as(select
date_trunc('day',RECORDED_HOUR) AS date1, 
avg(close) AS price
from crosschain.core.fact_hourly_prices
where ID = 'weth'
group by 1
  ), main as
(select 
block_timestamp::date as date,
price,
count(DISTINCT from_address) as user_count,
count(DISTINCT tx_hash) as tx_count,
count(DISTINCT(case when status = 'SUCCESS' then tx_hash end)) as tx_success_count,
avg(TX_FEE*price) as avg_fee
from optimism.core.fact_transactions a join price b on date_trunc('day',a.block_timestamp)=b.date1
group by 1,2)
select 
date,
price,
tx_count,
user_count,
avg_fee,
tx_success_count*100/tx_count as success_rate,
100 - success_rate as fail_rate,
tx_count/24*60 as tpm,
tx_count/24*60*60 as tps
from main 
"""
df = querying_pagination(df_query)


#total tx
df1_query="""
with price as(select
date_trunc('day',RECORDED_HOUR) AS date1, 
avg(close) AS price
from crosschain.core.fact_hourly_prices
where ID = 'weth'
group by 1
  )
select 
count(DISTINCT from_address) as user_count,
count(DISTINCT tx_hash) as tx_count,
count(DISTINCT(case when status = 'SUCCESS' then tx_hash end)) as tx_success_count,
avg(TX_FEE) as avg_fee
from optimism.core.fact_transactions
"""
df1 = querying_pagination(df1_query)

st.write("""
 # Overview #

 Here overal metric (Transaction / user count) is analyzed.

 """
)

cc1, cc2,cc3 = st.columns([1, 1,1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df1["user_count"][0]),
    label="Total user count",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df1["tx_count"][0]),
    label="Total transaction count",
)

with cc3:
  st.metric(
    value="{0:,.0f}".format(df1["tx_success_count"][0]),
    label="Total success transaction count",
)
st.subheader('Daily transaction count')
st.caption('Daily transaction count')
st.bar_chart(df, x='date', y = 'tx_count', width = 400, height = 400)

st.subheader('Daily user count')
st.caption('Daily user count')
st.bar_chart(df, x='date', y = 'user_count', width = 400, height = 400)

st.subheader('Daily average transaction fee')
st.caption('Daily average transaction fee')
st.line_chart(df, x='date', y = 'avg_fee', width = 400, height = 400)

st.write("""
 # Performance #

 Here network performance metric (Transaction success / fail rate & transaction per minute / secound) is analyzed.

 """
)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('Success rate')
 st.caption('Daily transaction success rate')
 st.line_chart(df, x='date', y = 'success_rate', width = 400, height = 400)

with cc2:
 st.subheader('Fail rate')
 st.caption('Daily transaction fail rate')
 st.line_chart(df, x='date', y = 'fail_rate', width = 400, height = 400)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('Total transaction per minute')
 st.caption('Total transaction per minute')
 st.line_chart(df, x='date', y = 'tpm', width = 400, height = 400)

with cc2:
 st.subheader('Total transaction per secound')
 st.caption('Total transaction per secound')
 st.line_chart(df, x='date', y = 'tps', width = 400, height = 400)