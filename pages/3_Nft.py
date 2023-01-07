import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="🌃 NFT (selling activity)",
    layout= "wide",
    page_icon="🌃",
)
st.title("🌃 NFT (selling activity)")
st.sidebar.success("🌃 NFT (selling activity)")


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
#daily nft sale
df_query="""
select 
block_timestamp::date as date,
count(tx_hash) as count_tx,
count(distinct buyer_address) as count_users,
sum(price) as volume,
sum(price_usd) as volume_usd
from optimism.core.ez_nft_sales
group by 1
"""
df = querying_pagination(df_query)


#total nft sale
df1_query="""
select 
count(tx_hash) as count_tx,
count(distinct buyer_address) as count_users,
sum(price) as volume,
sum(price_usd) as volume_usd
from optimism.core.ez_nft_sales
"""
df1 = querying_pagination(df1_query)

#user categorize by count of sale
df2_query="""
with main as (select 
distinct buyer_address as user,
count(tx_hash) as "count of transaction",
sum(price_usd) as "sales volume (USD)"
from optimism.core.ez_nft_sales
group by 1 
  )
select 
case
when "count of transaction" = 1 then 'transaction just once time' 
when "count of transaction"  BETWEEN 2 and 5 then 'transaction 2 - 5 time'  
when "count of transaction"  BETWEEN 6 and 15 then 'transaction 5 - 15 time'  
when "count of transaction" BETWEEN 16 and 50 then 'transaction 15 - 50 time'  
when "count of transaction" BETWEEN 51 and 100 then 'transaction 50 - 100 time'  
when "count of transaction" BETWEEN 101 and 200 then 'transaction 100 - 200 time'  
else 'transaction more than 200 time' end as tier,
count(distinct user) as count_buyer
from main
where tier is not null
group by 1
"""
df2 = querying_pagination(df2_query)

#user categorize by volume (USD) of sale
df3_query="""
with main as (select 
distinct buyer_address as user,
count(tx_hash) as count_tx,
sum(price_usd) as "sales volume (USD)"
from optimism.core.ez_nft_sales
group by 1 
  )
select 
case
when "sales volume (USD)" BETWEEN 0 and 10 then 'Shrimp 0-10 USD' 
when "sales volume (USD)"  BETWEEN 10 and 100 then 'Crab 10-100 USD'  
when "sales volume (USD)"  BETWEEN 100 and 500 then 'Octopus 100-500 USD'  
when "sales volume (USD)" BETWEEN 500 and 1000 then 'Fish 500-1k USD'  
when "sales volume (USD)" BETWEEN 1000 and 10000 then 'Dolphin 1k-10k USD'  
when "sales volume (USD)" BETWEEN 10000 and 100000 then 'Shark 10k-100k USD'  
else 'Whale >100k USD' end as tier,
count(distinct user) as count_buyer
from main
where tier is not null
group by 1
"""
df3 = querying_pagination(df3_query)

#top 10 collection by count of sale
df4_query="""
select 
distinct project_name as collection,
count(tx_hash) as count_tx
from optimism.core.ez_nft_sales a join optimism.core.dim_labels b on b.address=a.nft_address
group by 1 
order by 2 desc 
limit 10
"""
df4 = querying_pagination(df4_query)

#top 10 collection by volume of sell
df5_query="""
select 
distinct project_name as collection,
sum(price) as volume
from optimism.core.ez_nft_sales a join optimism.core.dim_labels b on b.address=a.nft_address
group by 1 
order by 2 desc 
limit 10
"""
df5 = querying_pagination(df5_query)

#top 10 user by count of sale
df6_query="""
select 
distinct buyer_address as user,
count(tx_hash) as count_tx
from optimism.core.ez_nft_sales
group by 1 
order by 2 desc 
limit 10
"""
df6 = querying_pagination(df6_query)

#top 10 user by volume (USD) of sale
df7_query="""
select 
distinct buyer_address as user,
sum(price) as volume
from optimism.core.ez_nft_sales
group by 1 
order by 2 desc 
limit 10
"""
df7 = querying_pagination(df7_query)

#flippers 1 week
df81_query="""
with main1 as (select 
project_name,
block_timestamp,
buyer_address
from optimism.core.ez_nft_sales a join optimism.core.dim_labels b on b.address=a.nft_address
),main2 as (select 
project_name,
block_timestamp,
seller_address
from optimism.core.ez_nft_sales a join optimism.core.dim_labels b on b.address=a.nft_address
)
select 
a.block_timestamp::date as date, 
count(distinct seller_address) as count_flippers
from main2 a inner join main1 b on a.seller_address = b.buyer_address and a.project_name = b.project_name
where datediff(day, a.block_timestamp, b.block_timestamp) <= 7
group by 1
"""
df81 = querying_pagination(df81_query)


#flippers 1 month
df9_query="""
with main1 as (select 
project_name,
block_timestamp,
buyer_address
from optimism.core.ez_nft_sales a join optimism.core.dim_labels b on b.address=a.nft_address
),main2 as (select 
project_name,
block_timestamp,
seller_address
from optimism.core.ez_nft_sales a join optimism.core.dim_labels b on b.address=a.nft_address
)
select 
a.block_timestamp::date as date, 
count(distinct seller_address) as count_flippers
from main2 a inner join main1 b on a.seller_address = b.buyer_address and a.project_name = b.project_name
where datediff(day, a.block_timestamp, b.block_timestamp) <= 30
group by 1
"""
df9 = querying_pagination(df9_query)

st.write("""
 # Overal nft sale activity #

 The Daily Number of sellers , sale count and sale volume (USD) metrics is a measure of how many wallets / transaction with how much volume (USD) on NEAR are making nft sale on chain.

 """
)

cc1, cc2 , cc3 , cc4= st.columns([1, 1 , 1,1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df1["count_tx"][0]),
    label="Total count of nft sale",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df1["count_users"][0]),
    label="Total count of nft sellers",
)
with cc3:
  st.metric(
    value="{0:,.0f}".format(df1["volume"][0]),
    label="Total volume (ETH) of nft sale",
)
with cc4:
  st.metric(
    value="{0:,.0f}".format(df1["volume_usd"][0]),
    label="Total volume (USD) of nft sale",
)


st.subheader('Daily count of nft sales')
st.caption('Daily count of nft sales')
st.bar_chart(df, x='date', y = 'count_tx', width = 400, height = 400)

st.subheader('Daily count of nft sellers')
st.caption('Daily count of nft sellers')
st.bar_chart(df, x='date', y = 'count_users', width = 400, height = 400)

st.subheader('Daily volume (ETH) of nft sales')
st.caption('Daily volume (ETH) of nft sales')
st.bar_chart(df, x='date', y = 'volume', width = 400, height = 400)

st.subheader('Daily volume (USD) of nft sales')
st.caption('Daily volume (USD) of nft sales')
st.bar_chart(df, x='date', y = 'volume_usd', width = 400, height = 400)

st.subheader('Daily flippers count who sold nfts less than one week / month on Optimism custom NFT.')
cc1, cc2 = st.columns([1, 1])

with cc1:
  st.caption('Daily flippers count who sold nfts less than one week on Optimism custom NFT.')
  st.line_chart(df81, x='date', y = 'count_flippers', width = 400, height = 400)
with cc2:
  st.caption('Daily flippers count who sold nfts less than one week on Optimism custom NFT.')
  st.line_chart(df9, x='date', y = 'count_flippers', width = 400, height = 400)

st.write("""
 # User categorize by count and volume (USD) of nft sale #

 Here the sellers are categorized based on the number and volume (USD) of the nft sale.

 """
)
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by count of nft sale')
 st.caption('User categorize by count of nft sale')
 st.bar_chart(df2, x='tier', y = 'count_buyer', width = 400, height = 400)
with cc2:
 fig = px.pie(df2, values='count_buyer', names='tier', title='User categorize by rate of nft sale count')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by volume (USD) of nft sale')
 st.caption('User categorize by volume (USD) of nft sale')
 st.bar_chart(df3, x='tier', y = 'count_buyer', width = 400, height = 400)
with cc2:
 fig = px.pie(df3, values='count_buyer', names='tier', title='User categorize by rate of nft sale volume (USD)')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

st.write("""
 # Top 10 collection #

 Top collection are based on the number and volume (USD) of the nft sale and sellers are:

 """
)
cc1, cc2 = st.columns([1,1])

with cc1:
 st.caption('Top 5 collection by count of nft sale')
 st.bar_chart(df4, x='collection', y = 'count_tx', width = 400, height = 400)

with cc2:
 st.caption('Top 5 collection by count of nft seller')
 st.bar_chart(df5, x='collection', y = 'volume', width = 400, height = 400)



st.write("""
 # Top 10 User #

 Top users are based on the number and volume (USD) of the nft sale are:

 """
)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.caption('Top 10 user by count of nft sale')
 st.bar_chart(df6, x='user', y = 'count_tx', width = 400, height = 400)
with cc2:
 st.caption('Top 10 user by volume (USD) of nft sale')
 st.bar_chart(df7, x='user', y = 'volume', width = 400, height = 400)
