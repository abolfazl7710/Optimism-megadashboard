import streamlit as st

st.set_page_config(
    page_title="Home",
    layout= "wide",
    page_icon="🏠",
)
st.title("🏠 Home")
st.sidebar.success("🏠 Home")

st.write("""
 # ❓Optimism Chain #
 # Methodology #
 In this dashboard, I used python to display charts.
 The data used was obtained from flipsidecrypto and queries link are in pages.
 All the codes of this web app can be seen in this link (Github). 
 
 https://github.com/abolfazl7710/Terradash
"""
)
st.write("""
 # 📝 Introduction #

 # What is Optimism? #
 Optimism is a layer 2 chain, meaning it functions on top of Ethereum mainnet (layer 1). Transactions take place on Optimism, but the data about transactions get posted to mainnet where they are validated. It's like driving in a less crowded side street while benefiting from the security of a highway.
 Optimism is the second-largest Ethereum layer 2 with a total of $313 million locked into its smart contracts, as of this writing, according to Defi Llama. Arbitrum comes first with $1.32 billion.
 Synthetix, a derivatives liquidity protocol, is the largest protocol on Optimism, with a total value locked (TVL) of $125 million. Uniswap, a decentralized exchange (DEX), is the second most popular protocol on the chain. As of this writing, there are 35 protocols on Optimism with at least $1,000 locked into their smart contracts.
 # How does Optimism work? #
 Optimism uses a technology called rollups, specifically Optimistic rollups.They're called rollups because they roll up (or bundle) the data about hundreds of transactions - non-fungible token (NFT) mints, token swaps … any transaction! - into a single transaction on Ethereum mainnet (layer 1). When so many transactions are rolled up into a single transaction, the blockchain transaction, or "gas," fee required to pay comes down to only one transaction, conveniently distributed across everyone involved.
 And they're called Optimistic rollups because transactions are assumed to be valid until they are proven false, or in other words, innocent until proven guilty. There's a time window during which potentially invalid transactions can be challenged by submitting a “fraud proof” and running the transactions computations with reference to available state data. Optimism reimburses the gas needed to run the computation of the fraud proof. 
  
 """
 )

st.write("")
st.write("")
st.write("")
st.write("📓 Contact data")
c1, c2 = st.columns(2)
with c1:
    st.info('**Twitter: [@daryoshali](https://twitter.com/daryoshali)**')
with c2:
    st.info('**Data: [Github](https://github.com/abolfazl7710)**')

st.write("")
st.write("")
st.write("")
st.write("Thanks for MetricsDAO and flipsidecrypto team")