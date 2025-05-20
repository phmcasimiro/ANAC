-- CRIAÇÃO DA TABELA dataset_anac
CREATE TABLE IF NOT EXISTS gisdb.desenvolvimento.dataset_anac
(
        numOcorrencia int,
        classifOcorrencia VARCHAR(50),
        dt_Ocorrencia DATE,
        nm_mun VARCHAR(50),
        uf VARCHAR(30),
        regiao VARCHAR(30),
        nm_Fabricante VARCHAR(100),
        lat float(11),
        long float(11)
);
-- CRIAÇÃO DA TABELA dataset_anac_geo
CREATE TABLE IF NOT EXISTS gisdb.desenvolvimento.dataset_anac_geo
(
        numOcorrencia int,
        classifOcorrencia VARCHAR(50),
        dt_Ocorrencia DATE,
        nm_mun VARCHAR(50),
        uf VARCHAR(30),
        regiao VARCHAR(30),
        nm_Fabricante VARCHAR(100),
        lat float(11),
        long float(11)
);

-- VERIFICAR SE HÁ REGISTROS NULOS NO ATRIBUTO "numocorrencia"
SELECT COUNT(*) AS total_registros_nulos 
FROM gisdb.desenvolvimento.dataset_anac_geo da
WHERE numocorrencia IS NULL;