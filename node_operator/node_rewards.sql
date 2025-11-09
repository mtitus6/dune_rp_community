/* Dune query number  - 5117116 */
select
    claimer as node_address,
    evt_block_time as t,
    amount_rpl / 1e18 as amount_rpl,
    (amount_rpl / 1e18) * rpl_price.token_price_usd as amount_rpl_usd,
    amount_eth / 1e18 as amount_eth,
    (amount_eth / 1e18) * reth_price.weth_price_usd as amount_eth_usd,
    reward_index,
    evt_tx_hash as tx_hash
from
    rocketpool_ethereum.rocketmerkledistributormainnet_evt_rewardsclaimed
cross join unnest(amountRPL, amountETH, rewardIndex) as u (amount_rpl, amount_eth, reward_index)
left join query_5106189 as rpl_price
    on
        date_trunc('hour', evt_block_time) = rpl_price.hr
left join query_3664567 as reth_price
    on
        date_trunc('hour', evt_block_time) = reth_price.hr
order by 1, 2, 7 asc
