
# Creación de esquea catastro2022_sgv donde se crea una capa de parcelas
# que contiene información de resumen obtenida a partir de:
# la capa de parcelas
# y de la tabla de edificios del trabajo del IECA
# Esto sirve para fabricar la capa de promociones de vivienda protegida localizadas


library("RPostgres")

con_owner <- dbConnect(RPostgres::Postgres(), 
                       dbname = 'dbgis01',  ##Nombre de la BBDD
                       host= 'VMGIS04.cfv.junta-andalucia.es',
                       port= '5444',
                       user= 'sige_owner'  , 
                       password= 'MCG3NtPM'  )  

#Creo el esquema
dbExecute(con_owner, "CREATE SCHEMA catastro2022_sgv 
                AUTHORIZATION sige_owner")

dbExecute(con_owner, "GRANT ALL ON SCHEMA catastro2022_sgv TO sige_owner")
dbExecute(con_owner, "GRANT USAGE ON SCHEMA catastro2022_sgv TO admingis_role")
dbExecute(con_owner, "GRANT ALL ON SCHEMA catastro2022_sgv TO sige_ajms")
# dbDisconnect(con_owner)




#ahora ya con el usuario sige_ajms creo las tablas
#ahora entro como sige_ajms y creo una tabla
# con_ajms <- dbConnect(RPostgres::Postgres(), 
#                       dbname = 'dbgis01',
#                       host= 'VMGIS04.cfv.junta-andalucia.es',
#                       port= '5444',
#                       user= 'sige_ajms', 
#                       password= 'Yrc39pbk')


orden <- "
CREATE TABLE catastro2022_sgv.parcelas_resumen AS
SELECT
    p.geom,
    p.delegacio as cat_delegacio,
    p.municipio as cat_municipio,
    p.refcat_d as cat_refcat_d,
    p.refcat as cat_refcat,
    p.via as cat_via,
    p.numero as cat_numero,
    p.sup_const_total as cat_sup_cont_total,
    MIN(e.cpro_ine) || MIN(e.cmun_ine) AS cat_codine,
    'V:' || SUM(e.nbi_v) || ' A:' || SUM(e.nbi_a) || ' Tot:' || SUM(e.nbi_tot) AS cat_inmuebles,
    SUM(e.nbi_v) AS cat_nbi_v,
    SUM(e.nbi_tot) AS cat_nbi_tot,
    SUM(e.nviv) AS cat_nviv_imputadas,
    MIN(e.ant_min) AS ant_min
FROM catastro2022.modelo_parcelas_r11 p
LEFT JOIN catastro2022.modelo_edificio_20221011 e ON p.refcat_d = e.rfcd_parcela
GROUP BY p.geom,p.refcat_d, p.delegacio, p.municipio, p.refcat, p.via, p.numero, p.sup_const_total
"
dbExecute(con_owner, orden)
# si necesita borrar para empezar de nuevo
#dbExecute(con_owner, "DROP TABLE catastro2022_sgv.parcelas_resumen")

# Creación de índices
indices <- c('
CREATE INDEX catastro2022_sgv_parcelas_codine
ON catastro2022_sgv.parcelas_resumen
USING btree
(cat_codine COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX catastro2022_sgv_parcelas_refcat_d
ON catastro2022_sgv.parcelas_resumen
USING btree
(cat_refcat_d COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX catastro2022_sgv_parcelas_refcat
ON catastro2022_sgv.parcelas_resumen
USING btree
(cat_refcat COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX catastro2022_sgv_parcelas_geom
ON catastro2022_sgv.parcelas_resumen
USING gist
(geom)
TABLESPACE idx01_admingis'
)

for (indice in indices){
  dbExecute(con_owner, indice)
}

privileg1 <- "
  GRANT SELECT ON TABLE catastro2022_sgv.parcelas_resumen TO admingis_role
"

privileg2 <- "
  GRANT SELECT, REFERENCES ON TABLE catastro2022_sgv.parcelas_resumen TO sige_consulta;
"

privileg3 <- "
  GRANT ALL ON TABLE catastro2022_sgv.parcelas_resumen TO sige_ajms;
"

dbExecute(con_owner, privileg1)
dbExecute(con_owner, privileg2)
dbExecute(con_owner, privileg3)

dbDisconnect(con_owner)
