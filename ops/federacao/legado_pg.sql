-- Lakehouse Federation do sistema legado (PostgreSQL externo).
-- A conexao guarda host, usuario e senha no Unity Catalog; a senha nunca entra no repositorio.
-- Criacao e rotacao de senha: scripts/federacao_conexao.sh (le o .env do provedor e recria conexao e catalogo).

-- Equivalente SQL da conexao (valores entre <> preenchidos so no workspace):
-- CREATE CONNECTION legado_pg TYPE postgresql
--   OPTIONS (host '<host>', port '5432', user '<usuario>', password '<senha>');

CREATE FOREIGN CATALOG IF NOT EXISTS legado_pg
  USING CONNECTION legado_pg
  OPTIONS (database 'neondb')
  COMMENT 'Catalogo federado do sistema legado de metas';

-- Governanca: o consumidor recebe leitura no catalogo, nunca a conexao.
-- GRANT USE CATALOG ON CATALOG legado_pg TO `<grupo_analytics>`;
-- GRANT USE SCHEMA ON SCHEMA legado_pg.public TO `<grupo_analytics>`;
-- GRANT SELECT ON TABLE legado_pg.public.metas_comerciais TO `<grupo_analytics>`;
-- USE CONNECTION / CREATE FOREIGN CATALOG ficam restritos ao dono da plataforma.

SELECT count(*) AS linhas, sum(meta_valor) AS meta_total FROM legado_pg.public.metas_comerciais;
