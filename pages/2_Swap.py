import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="💱 Swap activity",
    layout= "wide",
    page_icon="💱",
)
st.title("💱 Swap activity")
st.sidebar.success("💱 Swap activity")
 

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
#daily swap
df_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
block_timestamp::date as date,
'openocean' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where date >= '2022-01-01'
and amount >= 0
group by 1,2,3
ORDER BY 6 DESC
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
block_timestamp::date as date,
'clipper' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where date >= '2022-01-01'
and amount >= 0
group by 1,2,3
ORDER BY 6 DESC
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
block_timestamp::date as date,
'beethoven' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where date >= '2022-01-01'
and amount >= 0
group by 1,2,3
ORDER BY 6 DESC
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
block_timestamp::date as date,
'1 inch' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where date >= '2022-01-01'
and amount >= 0
group by 1,2,3
ORDER BY 6 DESC 
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
block_timestamp::date as date,
'Uniswap' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where date >= '2022-01-01'
and amount >= 0
group by 1,2,3
ORDER BY 6 DESC )
select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap
"""
df = querying_pagination(df_query)

#Total swap
df1_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'openocean' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as count_swaps,
count(distinct origin_from_address) as count_swappers,
sum(coalesce(amount,0)) as swapped_volume
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'clipper' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as count_swaps,
count(distinct origin_from_address) as count_swappers,
sum(coalesce(amount,0)) as swapped_volume
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'beethoven' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as count_swaps,
count(distinct origin_from_address) as count_swappers,
sum(coalesce(amount,0)) as swapped_volume
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'1 inch' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as count_swaps,
count(distinct origin_from_address) as count_swappers,
sum(coalesce(amount,0)) as swapped_volume
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC 
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'Uniswap' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as count_swaps,
count(distinct origin_from_address) as count_swappers,
sum(coalesce(amount,0)) as swapped_volume
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC )
select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap
"""
df1 = querying_pagination(df1_query)

#cat by count
df2_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'openocean' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'clipper' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'beethoven' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'1 inch' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'Uniswap' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2 ), main as(
select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap)
select
case 
when "count of swaps" = 1 then 'Swap just one time'
when "count of swaps" between 2 and 5 then 'Swap 2 - 5 time'
when "count of swaps" between 6 and 10 then 'Swap 6 - 10 time'
when "count of swaps" between 11 and 20 then 'Swap 11 - 20 time'
when "count of swaps" between 21 and 50 then 'Swap 21 - 50 time'
when "count of swaps" between 51 and 100 then 'Swap 51 - 100 time'
when "count of swaps" between 101 and 200 then 'Swap 101 - 200 time'
when "count of swaps" between 201 and 500 then 'Swap 201 - 500 time'
when "count of swaps" between 501 and 1000 then 'Swap 501 - 1000 time'
else 'Swap more than 1000 time'
end as count_swaps,
dex,
count(user) as swapper_count
from main
group by 1,2
"""
df2 = querying_pagination(df2_query)

#categorize (count of vol)
df3_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'openocean' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'clipper' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'beethoven' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'1 inch' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'Uniswap' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume (USD)"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2 ), main as(
select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap)
select
case 
when "swapped volume (USD)" < 10 then 'Swap less than 10 USD'
when "swapped volume (USD)" between 10 and 49.99 then 'Swap 10 - 50 USD'
when "swapped volume (USD)" between 50 and 99.99 then 'Swap 50 - 100 USD'
when "swapped volume (USD)" between 100 and 499.99 then 'Swap 100 - 500 USD'
when "swapped volume (USD)" between 500 and 999.99 then 'Swap 500 - 1000 USD'
when "swapped volume (USD)" between 1000 and 1999.99 then 'Swap 1000 - 2000 USD'
when "swapped volume (USD)" between 2000 and 4999.99 then 'Swap 2000 - 5000 USD'
when "swapped volume (USD)" between 5000 and 9999.99 then 'Swap 5000 - 10000 USD'
when "swapped volume (USD)" between 10000 and 19999.99 then 'Swap 10000 - 20000 USD'
when "swapped volume (USD)" between 20000 and 49999.99 then 'Swap 20000 - 50000 USD'
when "swapped volume (USD)" between 50000 and 99999.99 then 'Swap 50000 - 100000 USD'
else 'Swap more than 100000 USD'
end as swap_volume,
dex,
count(user) as swapper_count
from main
group by 1,2
"""
df3 = querying_pagination(df3_query)

#top pool (count of swap)
df4_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'openocean' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'clipper' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'beethoven' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'1 inch' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC 
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'Uniswap' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC ), main as
(select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap)
select 
* 
from main 
order by 3 desc 
limit 10
"""
df4 = querying_pagination(df4_query)

#Top pool by count swapper
df5_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'openocean' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'clipper' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'beethoven' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'1 inch' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC 
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'Uniswap' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC ), main as
(select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap)
select 
* 
from main 
order by 4 desc 
limit 10
"""
df5 = querying_pagination(df5_query)

#Top pool by volume
df6_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'openocean' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'clipper' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'beethoven' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'1 inch' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC 
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
'Uniswap' as dex,
concat(symbol_in, ' - ', symbol_out) as pool,
count(distinct tx_hash) as "count of swaps",
count(distinct origin_from_address) as "count of swappers",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
ORDER BY 5 DESC ), main as
(select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap)
select 
* 
from main 
order by 5 desc 
limit 10
"""
df6 = querying_pagination(df6_query)

#Top 10 user by count
df7_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'openocean' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'clipper' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'beethoven' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'1 inch' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'Uniswap' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2 ), main as(
select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap)
select
* 
from main 
order by 3 desc 
limit 10
"""
df7 = querying_pagination(df7_query)

#Top 10 user by volume
df8_query="""
with openocean as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x6352a56caadC4F1E25CD6c75970Fa768A3304e64') 
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x90411a32'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'openocean' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), clipper as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x5130f6cE257B8F9bF7fac0A0b519Bd588120ed40') 
and TOPICS[0]='0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'clipper' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
), beethoven as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x52bbbe29'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'beethoven' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), one_inch as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0x1111111254760F7ab3F16433eea9304126DCd199') 
and event_name in ('Swap' , 'Uniswap V3Swap')
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'1 inch' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2
  ), uniswap as (with price as (select
date_trunc('day', hour) as date,
token_address,
symbol,
decimals,
avg(price) as price
from optimism.core.fact_hourly_token_prices
group by 1,2,3,4
),txn_main as (select 
tx_hash,
rank() over (partition by tx_hash order by event_index desc ) as rank
from optimism.core.fact_event_logs 
where origin_to_address = lower('0xE592427A0AEce92De3Edee1F18E0157C05861564') or origin_to_address = lower('0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45')
and event_name = 'Swap'
qualify rank = 1 
),txn as (select 
b.block_timestamp,
b.origin_from_address,
b.tx_hash,
b.contract_address,
b.event_inputs,
rank() over (partition by b.tx_hash order by b.event_index) as rank
from txn_main a join optimism.core.fact_event_logs b on a.tx_hash = b.tx_hash
and b.event_name = 'Transfer'
and b.origin_function_signature = '0x414bf389'
order by b.tx_hash, b.event_index, rank
),swap as (select
a.block_timestamp,
a.origin_from_address,
a.tx_hash,
a.contract_address as from_token,
b.contract_address as to_token,
a.event_inputs:value as token_amount_in
from txn a join txn b on a.tx_hash = b.tx_hash and a.rank = 1 and b.rank = 2
  ),swap_txn as (select
block_timestamp,
tx_hash,
origin_from_address,
b.symbol as symbol_in,
c.symbol as symbol_out,
(a.token_amount_in/pow(10,d.decimals)) * d.price as amount
from swap a join optimism.core.dim_contracts b on a.from_token = b.address
join optimism.core.dim_contracts c on a.to_token = c.address
join price d on a.from_token = d.token_address and a.block_timestamp::date = d.date
)
select
distinct origin_from_address as user,
'Uniswap' as dex,
count(distinct tx_hash) as "count of swaps",
sum(coalesce(amount,0)) as "swapped volume"
from swap_txn
where block_timestamp >= '2022-01-01'
and amount >= 0
group by 1,2 ), main as(
select 
* 
from openocean
union 
select 
* 
from clipper
union 
select 
* 
from beethoven
union
select 
* 
from one_inch
union
select 
* 
from uniswap)
select
* 
from main 
order by 4 desc 
limit 10
"""
df8 = querying_pagination(df8_query)

options = st.multiselect(
    '**Select your desired dex:**',
    options=df['dex'].unique(),
    default=df['dex'].unique(),
    key='Dex'
)

if len(options) > 0:
 
 df = df.query("dex == @options")
 df1 = df1.query("dex == @options")
 df2 = df2.query("dex == @options")
 df3 = df3.query("dex == @options")
 df4 = df4.query("dex == @options")
 df5 = df5.query("dex == @options")
 df6 = df6.query("dex == @options")
 df7 = df7.query("dex == @options")
 df8 = df8.query("dex == @options")
 
 st.write("""
 # Overal swap activity #

 The Daily Number of swappers , swap count and swap volume (USD) metrics is a measure of how many wallets / transaction with how much volume (USD) on Optimism are making swap on chain.

 """
 )
 st.subheader('Total number of swap at each dex')
 cc1, cc2= st.columns([1, 1])
 
 with cc1:
  st.caption('Total count of swap at each dex')
  st.bar_chart(df1, x='dex', y = 'count_swaps', width = 400, height = 400)
 with cc2:
  fig = px.pie(df1, values='count_swaps', names='dex', title='Total count of swap at each dex')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 fig = px.bar(df, x='date', y='count of swaps',color='dex', title='Daily count of swap at each dex')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 st.subheader('Total number of swapper at each dex')
 cc1, cc2= st.columns([1, 1])
 
 with cc1:
  st.caption('Total count of swapper at each dex')
  st.bar_chart(df1, x='dex', y = 'count_swappers', width = 400, height = 400)
 with cc2:
  fig = px.pie(df1, values='count_swappers', names='dex', title='Total count of swapper at each dex')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 fig = px.bar(df, x='date', y='count of swappers',color='dex', title='Daily count of swapper at each dex')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 st.subheader('Total volume of swap at each dex')
 cc1, cc2= st.columns([1, 1])
 
 with cc1:
  st.caption('Total count of swapper at each dex')
  st.bar_chart(df1, x='dex', y = 'swapped_volume', width = 400, height = 400)
 with cc2:
  fig = px.pie(df1, values='swapped_volume', names='dex', title='Total volume of swap at each dex')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')
  
 fig = px.bar(df, x='date', y='swapped volume',color='dex', title='Daily volume of swap at each dex')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 st.write("""
 # Swap activity at each pool #

 The Daily Number of swappers , swap count and swap volume (USD) metrics is a measure of how many wallets / transaction with how much volume (USD) on Optimism are making swap at each pool.

 """
 )
 fig = px.bar(df1, x='pool', y='count_swaps',color='dex', title='Total count of swaps at each pool')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 fig = px.bar(df1, x='pool', y='count_swappers',color='dex', title='Total count of swappers at each pool')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 fig = px.bar(df1, x='pool', y='swapped_volume',color='dex', title='Total volume (USD) of swaps at each pool')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 st.write("""
 # User categorize by count and volume (USD) of swap #

 Here the swappers are categorized based on the number and volume (USD) of the swap.

 """
 )
 cc1, cc2 = st.columns([1, 1])

 with cc1:
  fig = px.bar(df2, x='count_swaps', y='swapper_count',color='dex', title='User categorize by rate of swap count')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 with cc2:
  fig = px.pie(df2, values='swapper_count', names='count_swaps', title='User categorize by rate of swap count')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme=None)

 cc1, cc2 = st.columns([1, 1])

 with cc1:
  fig = px.bar(df3, x='swap_volume', y='swapper_count',color='dex', title='User categorize by rate of swap volume (USD)')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 with cc2:
  fig = px.pie(df3, values='swapper_count', names='swap_volume', title='User categorize by rate of swap volume (USD)')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme=None)

 st.write("""
 # Top 10 pool #

 Top pool are based on the number and volume (USD) of the swap are:

 """
 )
 cc1, cc2 ,cc3= st.columns([1, 1,1])

 with cc1:
  fig = px.bar(df4, x='pool', y='count of swaps',color='dex', title='Top 10 pool by count of swap')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 with cc2:
  fig = px.bar(df5, x='pool', y='count of swappers',color='dex', title='Top 10 pool by count of swappers')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')
 with cc3:
  fig = px.bar(df6, x='pool', y='swapped volume',color='dex', title='Top 10 pool by volume (USD) of swap')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

 st.write("""
 # Top 10 User #

 Top users are based on the number and volume (USD) of the swap are:

 """
 )
 cc1, cc2 = st.columns([1, 1])

 with cc1:

  st.subheader('Top 10 Swapper by count of swap')
  st.caption('Top 10 Swapper by count of swap')
  st.bar_chart(df7, x='user', y = 'count of swaps', width = 400, height = 400)
 with cc2:
  st.subheader('Top 10 Swapper by volume (USD) of swap')
  st.caption('Top 10 Swapper by volume (USD) of swap')
  st.bar_chart(df8, x='user', y = 'swapped volume', width = 400, height = 400)