-- Cenarios do agente de quarentena: copias das tabelas originais em schemas *_cen,
-- com anomalias G2 e G4 aplicadas por SQL. Leitura nos originais, escrita apenas em *_cen.
CREATE SCHEMA IF NOT EXISTS workspace.bronze_cen COMMENT 'Cenarios simulados - descartavel';
CREATE SCHEMA IF NOT EXISTS workspace.silver_cen COMMENT 'Cenarios simulados - descartavel';
CREATE SCHEMA IF NOT EXISTS workspace.gold_cen COMMENT 'Cenarios simulados - descartavel';
CREATE SCHEMA IF NOT EXISTS workspace.control_cen COMMENT 'Cenarios simulados - descartavel';

CREATE OR REPLACE TABLE workspace.bronze_cen.bronze_erp_pedidos_cabecalho AS SELECT * FROM workspace.bronze.bronze_erp_pedidos_cabecalho;
CREATE OR REPLACE TABLE workspace.silver_cen.silver_pedidos_cabecalho AS SELECT * FROM workspace.silver.silver_pedidos_cabecalho;
CREATE OR REPLACE TABLE workspace.silver_cen.tabelas_invalidas AS SELECT * FROM workspace.silver.tabelas_invalidas;
CREATE OR REPLACE TABLE workspace.silver_cen.tabelas_orfas AS SELECT * FROM workspace.silver.tabelas_orfas;
CREATE OR REPLACE TABLE workspace.silver_cen.silver_canais AS SELECT * FROM workspace.silver.silver_canais;
CREATE OR REPLACE TABLE workspace.gold_cen.dim_canal AS SELECT * FROM workspace.gold.dim_canal;
CREATE OR REPLACE TABLE workspace.control_cen.ingestion_log AS SELECT * FROM workspace.control.ingestion_log;

-- G2: variante de dominio 'SUL.' (fonte com 'Sul.' em vez de 'sul') chega a Silver e a Gold.
CREATE OR REPLACE TABLE workspace.silver_cen.silver_regioes AS
SELECT * FROM workspace.silver.silver_regioes
UNION ALL
SELECT 'SUL.', 'Região Sul', 'Sta Catarina', 'Rafael Souza', current_timestamp();

CREATE OR REPLACE TABLE workspace.gold_cen.dim_regiao AS
SELECT * FROM workspace.gold.dim_regiao
UNION ALL
SELECT sha2('SUL.', 256), 'SUL.', 'Região Sul', 'Sta Catarina', 'Rafael Souza';

-- G4: vendedores.csv recebido com 5 colunas; validacao de layout registra NOK.
INSERT INTO workspace.control_cen.ingestion_log
    (source_file, target_table, status, records_loaded, pipeline_execution_timestamp,
     validation_message, detected_schema, expected_schema)
VALUES ('vendedores.csv', 'bronze.bronze_comercial_vendedores', 'NOK', 0, current_timestamp(),
        'cenario G4 simulado: layout divergente', 'sep=; cols=5', 'sep=; cols=6');
