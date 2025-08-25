-- Current service fees for contracts with business logic for special cases
-- Target table: watson.shared_contract_service_fees_dbt

with trx_data as (
    select
        ax.related_assignment::bigint as contract_term_id,
        t.contract_id,
        ax.service_fee_pct
    from SHASTA_SDC_UPWORK.odesk_bpa_odesk_db.accountingtrx ax
    join SHASTA_SDC_UPWORK.contracts.terms t on ax.related_assignment::bigint = t.id::bigint
    where
        ax.type = 'APAdjustment'
        and ax.accounting_subtype = 'Service Fee'
        and ax.service_fee_pct is not null
    qualify row_number() over(partition by t.contract_id order by ax.date_created desc nulls last) = 1 -- latest known fee
)

select
    c.id as contract_id,
    case
        when tx.service_fee_pct is not null then tx.service_fee_pct
        when trc.rate_percent is not null then trc.rate_percent
        /* hardcoding L1s where service fee was raised */
        when o.group_uid in ('531770282580668416','531770282584862720','531770282580668423') then 15.00
        /* setting lowered fees for specific delivery models */
        when c.delivery_model ilike 'DIRECT_CONTRACT%' then 5.00
        when c.delivery_model ilike 'UPWORK_REMOTE' then 0.00
        else 10.00
    end as service_fee_percent
from SHASTA_SDC_UPWORK.contracts.contracts c
join SHASTA_SDC_UPWORK.openings.openings o on c.opening_uid = o.uid
left join trx_data tx on c.id = tx.contract_id
left join SHASTA_SDC_UPWORK.monetization_service_fee.talent_fee_contract_overrides tfo on tfo.parent_contract_id = c.id
left join SHASTA_SDC_UPWORK.monetization_service_fee.talent_fee_rate_cards trc on trc.reason_code = tfo.talent_fee_reason 