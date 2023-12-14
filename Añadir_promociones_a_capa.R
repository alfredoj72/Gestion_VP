# AÑADIR ELEMENTOS A PROMOCIONES Y A PROMOCIONES_PARCELAS
# 
library(rio)
library(tidyverse)
library(sf) 
library(RPostgres)

# Lee el fichero de promociones más reciente

patron <-  "^promociones_a_localizar.*\\.xlsx$"
archivos <- list.files("./promociones/", full.names = TRUE, pattern = patron )

# Si hay archivos que cumplan con el filtro
if (length(archivos) > 0) {
  # Ordena los archivos alfabéticamente y toma el último
  archivo <- tail(sort(archivos), 1)
  
# Ahora puedes leer el archivo XLSX
  promociones <- import(archivo)
} else {
  cat("No se encontraron archivos promociones_a_localizar-xxxxxx.xlsx.\n")
}

rm(archivo, archivos, patron)  

promociones_a_crear <- promociones %>% 
  filter(asig_verificado == 1)

if (nrow(promociones_a_crear) == 0) {
  cat("No existen registros de promociones con información de parcelas")
}

#obtener las referencias catastrales
rc <- promociones_a_crear %>%  pull(asig_refcat_d)


# Convierte la lista de códigos en una cadena separada por comas para la consulta SQL
rc_como_cadena <- paste0("'", rc, "'", collapse = ",")

#Crear registro para promociones_parcelas
#Añadir registro a promociones_parcelas

con_ajms <- dbConnect(RPostgres::Postgres(), 
                      dbname = 'dbgis01',
                      host= 'VMGIS04.cfv.junta-andalucia.es',
                      port= '5444',
                      user= 'sige_ajms', 
                      password= 'Yrc39pbk')


# Construye la consulta SQL
consulta_sql <- paste("SELECT * FROM catastro2022_sgv.parcelas_resumen
                      WHERE cat_refcat_d IN (", rc_como_cadena, ")", sep = "")

# Leer como capas con información geográfica
# ## seleccionar solo las referencia catastrales involucradas.
parcelas <- st_read(con_ajms, 
                          query = consulta_sql,
                          geometry_column  = "geom")

 
promociones_a_crear <- promociones_a_crear %>% 
  left_join(parcelas, by = c("asig_refcat_d" = "cat_refcat_d" ))

rm(rc, rc_como_cadena, consulta_sql, parcelas)

promociones_a_crear <- st_as_sf(promociones_a_crear, sf_column_name = "geom")


####    Crear registro para promociones  ####
#-----------------------------------------------------------------------
para_promociones <- promociones_a_crear %>% 
  group_by(prom_exp_po) %>% 
  summarise(
    prom_id_po = first(id_po),
    prom_f_calif_prov = as.Date(first(prom_f_calif_prov), "%d/%m/%y"),
    prom_f_calif_def = as.Date(first(prom_f_calif_def), "%d/%m/%y"),
    prom_f_archivo = NA,
    prom_nif_prom = first(prom_nif_prom),
    prom_denom_prom = first(prom_denom_prom),
    prom_direccion = first(prom_direccion),
    prom_codine = first(prom_codine),
    prom_nviviendas = first(prom_nviviendas),
    prom_num_dni = NA,
    asig_nviv_localizadas = sum(as.numeric(asig_nviv_localizadas), na.rm = TRUE),
    prom_num_adquisiciones = NA,
    prom_uso = first(prom_uso),
    cat_nviv_imputadas = sum(cat_nviv_imputadas, na.rm = TRUE)
  ) %>% 
  ungroup()

st_geometry(para_promociones) <- "the_geom"   #renombro la columna de geometría
para_promociones <- st_cast(para_promociones, "MULTIPOLYGON")


#Añadir registro a promociones
st_write(para_promociones, dsn = con_ajms, 
         Id(schema="viv_vp", table = "promociones"),
         append = TRUE)


####    Crear registros para promociones_parcelas  ####
#-----------------------------------------------------------------------

para_promociones_parcela <- data.frame(  
  prom_gid = NA,
  prom_id_po = promociones_a_crear$id_po,
  prom_exp_po = promociones_a_crear$prom_exp_po,
  prom_f_calif_def = as.Date(promociones_a_crear$prom_f_calif_def, "%d/%m/%y"),
  prom_nif_prom = promociones_a_crear$prom_nif_prom,
  prom_denom_prom = promociones_a_crear$prom_denom_prom,
  prom_direccion = promociones_a_crear$prom_direccion,
  prom_nviviendas = promociones_a_crear$prom_nviviendas,
  prom_codine = promociones_a_crear$prom_codine,
  cat_delegacio = promociones_a_crear$cat_delegacio,
  cat_municipio = promociones_a_crear$cat_municipio,
  cat_refcat = promociones_a_crear$cat_refcat,           
  prom_num_dni = NA,
  cat_via = promociones_a_crear$cat_via,
  cat_numero = promociones_a_crear$cat_numero,
# geom = promociones_a_crear$geom,  #aunque le ponga nombre siempre llama geometry al campo con la geometría
  cat_denomvia = NA,
  cat_nbi_tot = promociones_a_crear$cat_nbi_tot,
  cat_inmuebles = promociones_a_crear$cat_inmuebles,
  cat_construcciones = NA,
  cat_sup_cont_total = promociones_a_crear$cat_sup_cont_total,
  asig_nviv_localizadas = promociones_a_crear$asig_nviv_localizadas,
  asig_verificado = promociones_a_crear$asig_verificado,
  cat_nbi_v = promociones_a_crear$cat_nbi_v,
  cat_nviv_imputadas = promociones_a_crear$cat_nviv_imputadas
)

st_geometry(para_promociones_parcela) <- st_geometry(promociones_a_crear)
st_geometry(para_promociones_parcela) <- "the_geom"

st_write(para_promociones_parcela, dsn = con_ajms, 
         Id(schema="viv_vp", table = "promociones_parcelas"),
         append = TRUE)




####  Generar nueva tabla de promociones a localizar quitando los ya localizados ####
#-----------------------------------------------------------------------

promociones_restantes <- promociones %>% 
  filter(asig_verificado != 1 | is.na(asig_verificado))

# Salva el fichero con la fecha y hora actuales en el nombre
fecha_hora_actual <- format(Sys.time(), format = "%Y%m%d %H%M")
nombre_archivo <- paste0("promociones_a_localizar ", fecha_hora_actual)

# Guarda el objeto con el nombre de archivo generado
# save(promociones_restantes, file = paste0("./promociones/", nombre_archivo, ".Rdata"))

writexl::write_xlsx(promociones_restantes,
                    paste0("./promociones/", nombre_archivo, ".xlsx"))


