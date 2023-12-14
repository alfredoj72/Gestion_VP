# Carga en memoria los datos de las promociones de vivienda protegida y
# realiza una serie de cálculos para obtener estadísticas por municipios


library("RPostgres")
library("tidyverse")
library("sf")

# Conexión
con_ajms <- dbConnect(RPostgres::Postgres(), 
                      dbname = 'dbgis01',
                      host= 'VMGIS04.cfv.junta-andalucia.es',
                      port= '5444',
                      user= 'sige_ajms', 
                      password= 'Yrc39pbk')


#Realizo la consulta para obtener cada referencia catastral con sus
#código de delegación y municipio del catastro,

consulta_sql <- "SELECT t1.*
             FROM viv_vp.promociones AS t1"

# Ejecutar la consulta y obtener los resultados
promociones <- dbGetQuery(con_ajms, consulta_sql)

consulta_sql <- "SELECT *
             FROM viv_vp.promociones_parcelas"

# Ejecutar la consulta y obtener los resultados
promociones_parcelas <- dbGetQuery(con_ajms, consulta_sql)


# Leer como capas con información geográfica
promociones_sf <- st_read(con_ajms, 
                          query = "SELECT * from viv_vp.promociones",
                          geometry_column  = "the_geom")

promociones_parcelas_sf <- st_read(con_ajms, 
                          query = "SELECT * from viv_vp.promociones_parcelas",
                          geometry_column  = "the_geom")

dbDisconnect(con_ajms)
rm(consulta_sql, con_ajms)


# CONSULTAS



anyo_inicio <- 1991

# Viviendas por año de calificación definitiva, expedientes no archivados
consulta <- promociones %>%
  mutate(f_calif_def = as.Date(f_calif_def)) %>% 
  filter(year(f_calif_def) > anyo_inicio) %>% 
  filter(is.na(f_archivo)) %>% 
  group_by(year(f_calif_def)) %>% 
  summarise(viviendas = sum(n_vivienda, na.rm= TRUE)) # %>% 
  pull(viviendas)


# Viviendas por municipio, expedientes no archivados
consulta <- promociones %>%
  mutate(f_calif_def = as.Date(f_calif_def),
         periodo = ifelse(year(f_calif_def)<=1993,"Hasta 1993","Desde 1994")) %>% 
  filter(is.na(f_archivo) & !is.na(f_calif_def)) %>% 
  group_by(cod_ine, periodo) %>% 
  summarise(viviendas = sum(n_vivienda, na.rm= TRUE)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = periodo, values_from = viviendas)
  
  
  
# Convertir en fecha el campo fecha_cali 
promociones_localizadas <- promociones_localizadas %>% 
  separate(fecha_cali, c("dia", "mes", "año"), sep = "/") %>% 
  mutate(año = as.numeric(año),
         año = ifelse(año <=22, año + 2000, año + 1900),
         fecha_cali = as.Date(paste0(año, "-", mes, "-", dia),
                              format = "%Y-%m-%d"))  
  
consulta <- promociones_localizadas %>%
  filter(year(fecha_cali) > anyo_inicio) %>% 
  group_by(year(fecha_cali)) %>% 
  summarise(viviendas = sum(n_vivienda, na.rm= TRUE)) # %>% pull(viviendas)


consulta <- promociones_localizadas %>%
  summarise(n_vivienda = sum(n_vivienda, na.rm= TRUE),
            num_localizadas = sum(num_localizadas, na.rm = TRUE),
            num_viviendas = sum(num_viviendas, na.rm = TRUE )) # %>% pull(viviendas)


consulta <- promociones_sf %>%
  mutate(f_calif_def = as.Date(f_calif_def)) %>% 
  filter(year(f_calif_def) > anyo_inicio) %>% 
  filter(is.na(f_archivo) & !st_is_empty(.)) %>% 
  group_by(anyo = year(f_calif_def)) %>% 
  summarise(viviendas = sum(n_vivienda, na.rm= TRUE))  %>%
  ungroup() %>% 
  st_drop_geometry()

writexl::write_xlsx(consulta, "vp.xlsx")

  
