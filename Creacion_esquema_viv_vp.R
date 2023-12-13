
# Creación de esquema viv_vp que contiene las capas de vivienda protegida:
# promociones_parcela (anteriormente promociones_jesus)
# promociones (anteriormente promociones2017)
# 

library("RPostgres")

con_owner <- dbConnect(RPostgres::Postgres(), 
                       dbname = 'dbgis01',  ##Nombre de la BBDD
                       host= 'VMGIS04.cfv.junta-andalucia.es',
                       port= '5444',
                       user= 'sige_owner'  , 
                       password= 'MCG3NtPM'  )  

#Creo el esquema
dbExecute(con_owner, "CREATE SCHEMA viv_vp 
                AUTHORIZATION sige_owner")

dbExecute(con_owner, "GRANT ALL ON SCHEMA viv_vp TO sige_owner")
dbExecute(con_owner, "GRANT USAGE ON SCHEMA viv_vp TO admingis_role")
dbExecute(con_owner, "GRANT ALL ON SCHEMA viv_vp TO sige_ajms")
#dbDisconnect(con_owner)

# #BORRO el esquema si es que ya existe
# dbExecute(con_owner, "DROP SCHEMA viv_vp CASCADE")


#ahora ya con el usuario sige_ajms creo las tablas
#ahora entro como sige_ajms y creo una tabla
# con_ajms <- dbConnect(RPostgres::Postgres(), 
#                       dbname = 'dbgis01',
#                       host= 'VMGIS04.cfv.junta-andalucia.es',
#                       port= '5444',
#                       user= 'sige_ajms', 
#                       password= 'Yrc39pbk')

orden <- "
CREATE TABLE viv_vp.promociones AS
SELECT *
FROM viv_vivienda_protegida.promociones2017
"
#dbExecute(con_ajms, orden)
dbExecute(con_owner, orden)


# Renombrado de campos de tabla promociones
ordenes <- 
c(
  "ALTER TABLE viv_vp.promociones RENAME COLUMN id_po TO prom_id_po",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN exp_po TO prom_exp_po",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN f_calif_prov TO prom_f_calif_prov",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN f_calif_def TO prom_f_calif_def",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN f_archivo TO prom_f_archivo",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN nif_promotor TO prom_nif_prom",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN promotor TO prom_denom_prom",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN direccion TO prom_direccion",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN cod_ine TO prom_codine",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN n_vivienda TO prom_nviviendas",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN num_dni TO prom_num_dni",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN n_asignadas TO asig_nviv_localizadas",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN num_adquisiciones TO prom_num_adquisiciones",
  "ALTER TABLE viv_vp.promociones RENAME COLUMN uso TO prom_uso",
#  "ALTER TABLE viv_vp.promociones RENAME COLUMN the_geom TO geometry",

  "ALTER TABLE viv_vp.promociones ADD COLUMN cat_nviv_imputadas integer",
  
  "ALTER TABLE viv_vp.promociones DROP COLUMN id_po_alt",
  "ALTER TABLE viv_vp.promociones DROP COLUMN exp_po_m1",
  "ALTER TABLE viv_vp.promociones DROP COLUMN exp_po_m2",
  "ALTER TABLE viv_vp.promociones DROP COLUMN exp_po_m3",
  "ALTER TABLE viv_vp.promociones DROP COLUMN agno",
  "ALTER TABLE viv_vp.promociones DROP COLUMN x_id_grupo",
  "ALTER TABLE viv_vp.promociones DROP COLUMN num_exp",
  "ALTER TABLE viv_vp.promociones DROP COLUMN cod_prov",
  "ALTER TABLE viv_vp.promociones DROP COLUMN fase"
)

for (orden in ordenes){
  dbExecute(con_owner, orden)
}

# Creación de índices
indices <- c('
CREATE INDEX promociones_exp_po_idx
ON viv_vp.promociones
USING btree
(prom_exp_po COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX promociones_codine_idx
ON viv_vp.promociones
USING btree
(prom_codine COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX promociones_geom_idx
ON viv_vp.promociones
USING gist
(the_geom)
TABLESPACE idx01_admingis'
)

for (indice in indices){
  dbExecute(con_owner, indice)
}

privileg1 <- "
  GRANT SELECT ON TABLE viv_vp.promociones TO admingis_role
"

privileg2 <- "
  GRANT SELECT, REFERENCES ON TABLE viv_vp.promociones TO sige_consulta;
"

privileg3 <- "
  GRANT ALL ON TABLE viv_vp.promociones TO sige_ajms;
"

dbExecute(con_owner, privileg1)
dbExecute(con_owner, privileg2)
dbExecute(con_owner, privileg3)




#####  PROMOCIONES_PARCELAS

orden <- "
CREATE TABLE viv_vp.promociones_parcelas AS
SELECT *
FROM viv_vivienda_protegida.promociones_jesus
"
#dbExecute(con_ajms, orden)
dbExecute(con_owner, orden)

# Renombrado de campos de tabla promociones_parcelas
# Renombro los campos para que sea más fácil conocer de que tabla proceden:
# 1- prom proceden de la tabla de promociones,
# 2- cat proceden de la tabla/capa de resumen de catastro
# 3- asig proceden del proceso de asignación de valores realizado en Qgis
# RECORDAR: Con QGis, cuando se localizan las parcelas sobre las que se ubica una
# promocion :Rellenar la referencia catastral, el número de viviendas de la promocion
# que se encuentran en dicha parcela catastral y el valor 1 en el campo asig_verificado


# Crear un campo prom_f_calif_def de tipo fecha y formato "dd/mm/aaaa"
orden <- "ALTER TABLE viv_vp.promociones_parcelas ADD COLUMN prom_f_calif_def DATE"
dbExecute(con_owner, orden)

# Actualizar el nuevo campo utilizando una expresión
orden <- "
UPDATE viv_vp.promociones_parcelas
SET prom_f_calif_def = 
  CASE 
WHEN EXTRACT(YEAR FROM TO_DATE(fecha_cali, 'DD/MM/YY')) >= 24 
THEN TO_DATE(fecha_cali, 'DD/MM/YY') 
ELSE TO_DATE(fecha_cali, 'DD/MM/YY') + INTERVAL '100 years'
END
"
dbExecute(con_owner, orden)

# Elimina el campo de fecha de calificacion definitiva antiguo
orden <-
"ALTER TABLE viv_vp.promociones_parcelas DROP COLUMN fecha_cali"
dbExecute(con_owner, orden)

ordenes <- 
  c(
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN gid TO prom_gid",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN id_po TO prom_id_po",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN exp_po TO prom_exp_po",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN nif_promot TO prom_nif_prom",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN denominaci TO prom_denom_prom",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN direccion TO prom_direccion",
    "ALTER TABLE viv_vp.promociones_parcelas DROP COLUMN cod_munic",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN n_vivienda TO prom_nviviendas",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN cod_ine TO prom_codine",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN num_dni TO prom_num_dni",
    
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN delegacio TO cat_delegacio",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN municipio TO cat_municipio",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN refcat TO cat_refcat",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN via TO cat_via",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN numero TO cat_numero",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN denomvia TO cat_denomvia",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN num_inmuebles TO cat_nbi_tot",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN inmuebles TO cat_inmuebles",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN construcciones TO cat_construcciones",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN m_construidos TO cat_sup_cont_total",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN num_viviendas TO cat_nbi_v",
    
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN num_localizadas TO asig_nviv_localizadas",
    "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN verificado TO asig_verificado",
 #   "ALTER TABLE viv_vp.promociones_parcelas RENAME COLUMN the_geom TO geometry",
    
    "ALTER TABLE viv_vp.promociones_parcelas ADD COLUMN cat_nviv_imputadas integer",
    "ALTER TABLE viv_vp.promociones ADD CONSTRAINT promociones_pkey PRIMARY KEY (prom_id_po)"
  )

for (orden in ordenes){
  dbExecute(con_owner, orden)
}

# Creación de índices
indices <- c('
CREATE INDEX promociones_parcelas_exp_po_idx
ON viv_vp.promociones_parcelas
USING btree
(prom_exp_po COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX promociones_parcelas_codine_idx
ON viv_vp.promociones_parcelas
USING btree
(prom_codine COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX promociones_parcelas_refcat_idx
ON viv_vp.promociones_parcelas
USING btree
(cat_refcat COLLATE pg_catalog."default")
TABLESPACE idx01_admingis',
'
CREATE INDEX promociones_parcelas_geom_idx
ON viv_vp.promociones_parcelas
USING gist
(the_geom)
TABLESPACE idx01_admingis'
)

for (indice in indices){
  dbExecute(con_owner, indice)
}

privileg1 <- "
  GRANT SELECT ON TABLE viv_vp.promociones_parcelas TO admingis_role
"

privileg2 <- "
  GRANT SELECT, REFERENCES ON TABLE viv_vp.promociones_parcelas TO sige_consulta;
"

privileg3 <- "
  GRANT ALL ON TABLE viv_vp.promociones_parcelas TO sige_ajms;
"

dbExecute(con_owner, privileg1)
dbExecute(con_owner, privileg2)
dbExecute(con_owner, privileg3)


################# ####################     ############################
# CÓDIGO PARA LEER LAS CAPAS

library("RPostgres") ; library("tidyverse") ; library("sf")

# Conexión
con_ajms <- dbConnect(RPostgres::Postgres(), 
                      dbname = 'dbgis01',
                      host= 'VMGIS04.cfv.junta-andalucia.es',
                      port= '5444',
                      user= 'sige_ajms', 
                      password= 'Yrc39pbk')

# Leer como capas con información geográfica
promociones_sf <- st_read(con_ajms, 
                          query = "SELECT * from viv_vp.promociones",
                          geometry_column  = "the_geom")

promociones_parcelas_sf <- st_read(con_ajms, 
                                   query = "SELECT * from viv_vp.promociones_parcelas",
                                   geometry_column  = "the_geom")

dbDisconnect(con_ajms)
rm(con_ajms)

