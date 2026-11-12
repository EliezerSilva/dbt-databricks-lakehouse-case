{{ config(severity='warn') }}
-- Sinaliza itens sem cabecalho (ver models/marts/itens_sem_cabecalho.sql).
select * from {{ ref('itens_sem_cabecalho') }}
