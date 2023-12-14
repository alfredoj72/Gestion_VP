# Tomaa como elemento de partida una tabla con promociones hecha por el
# Servicio de vivienda a partir de información previa y de Gestor VP
# 
# Esta tabla obedece al formato que se remitió desde el Servicio de Vivienda
# a las delegaciones territoriales para completar y mejorar la información
# de Gestor VP.
# En concreto en esta ocasión se toma como punto de partida la información 
# recopilada en diciembre de 2023 con todos las promociones que desde 2019
# se han calificado provisional o definitivamente.
# Esta recopilación se realizó para enviar la información estadística al 
# Ministerio de Fomento.

# El presente script genera una tabla de salida con nombre
# promociones_a_localizar aaaammdd hhmm.xlsx (dia, mes, año,....)
# En dicha tabla hay que completar la información de los campos:
# asig_refcat_d, asig_nviv_localizadas, asig_verificado
# para poder usa el script Añadir_promociones_a_capa.R y con ello añadir
# los nuevos elementos a la capa


library(rio)
library(janitor)
library(tidyverse)

# preparo tabla para añadir código de municipo a la tabla de promociones a partir
# del nombre del municipio. Para realizar el enlace elimino los artículos y 
# el texto (capital) que aparece en las capitales de provincia

municipios_2022 <- read.csv("datos_aux/datos_poblacion_2022.txt", 
                                   header= TRUE,
                                   sep = ";",
                                   colClasses = "character")
  
municipios_2022 <- municipios_2022 %>% 
  rename(municipio = Lugar.de.residencia,
         prom_codine = CODIGO_INE3) %>% 
  mutate(municipio = toupper(municipio)) %>% 
  select(municipio, prom_codine)


# Elimina los artículos del inicio del nombre y el texto " (CAPITAL)" si existen
municipios_2022 <- municipios_2022 %>% 
  mutate(municipio = sub("^\\b(EL|LA|LOS|LAS)\\b\\s*|\\s*\\(CAPITAL\\)$", "", municipio),
         municipio = trimws(municipio),
         municipio = iconv(municipio, to = "ASCII//TRANSLIT")  #quita acentos
         )



# Cargo la tabla con las promociones a localizar

archivo <- "./promociones/promociones_con_calif_def_2019.2023.xlsx"
promociones_a_localizar <- import(archivo, skip = 0)

# falla con los campos fecha
# promociones_a_localizar <- promociones_a_localizar %>%
#  mutate_all(~ifelse(. == "(vacío)", NA, .))


# Adapto los nombres a lo que necesito para  crear la capa de promociones localizadas
# y creo campos vacios para incluir información de la localización (en Qgis)
# 
promociones_a_localizar <- promociones_a_localizar %>% 
  clean_names() %>% 
  mutate(prom_uso = paste(regimen, uso_principal, uso_detallado)) %>% 
  rename(
    prom_tenencia = tenencia,
    prom_tipo_promotor = promotor,
    prom_exp_po = codigo,
    prom_f_calif_prov = f_provisional,
    prom_f_calif_def = f_definitiva,
    prom_nif_prom = cod,
    prom_denom_prom = nombre_o_razon_social_del_promotor,
    prom_direccion = direccion,
    prom_municipio = municipio,
    prom_nviv_total = viviendas_total,
    prom_nviv_alquiler = viviendas_alquiler,
    prom_nviv_venta = viviendas_venta
    ) %>% 
  mutate(
    asig_refcat_d = as.character(NA),
    asig_nviv_localizadas = NA,
    asig_verificado = as.character(NA)
  )


promociones_a_localizar <- promociones_a_localizar %>% 
  mutate(municipio_enlace = toupper(prom_municipio),
         municipio_enlace = sub("\\s*\\(EL\\)|\\s*\\(LA\\)|\\s*\\(LOS\\)|\\s*\\(LAS\\)$", "", municipio_enlace), #quita el artículo del final
         municipio_enlace = sub("^\\b(EL|LA|LOS|LAS)\\b\\s*", "", municipio_enlace),  #quita el artículo también del principio
         municipio_enlace = iconv(municipio_enlace, to = "ASCII//TRANSLIT"),
         municipio_enlace = trimws(municipio_enlace)
  )
         
     


promociones_a_localizar <-  left_join(
  promociones_a_localizar,
  municipios_2022,
  by = c("municipio_enlace" = "municipio")
  ) %>% 
  mutate(municipio_enlace = NULL) %>% 
  relocate(prom_codine, .before = "asig_refcat_d")

rm(archivo, municipios_2022)

# creo nuevo campo identificador numerico
inicio <- 100000  # primer valor a dar al campo
id_po <- seq(inicio, length.out = nrow(promociones_a_localizar))

# Insertar el nuevo campo al principio del DataFrame
promociones_a_localizar <- data.frame(id_po = id_po, promociones_a_localizar)


# Salva el fichero con la fecha y hora actuales en el nombre
fecha_hora_actual <- format(Sys.time(), format = "%Y%m%d %H%M")
nombre_archivo <- paste0("promociones_a_localizar ", fecha_hora_actual)

# Guarda el objeto con el nombre de archivo generado
# save(promociones_a_localizar, file = paste0("./promociones/", nombre_archivo, ".Rdata"))

writexl::write_xlsx(promociones_a_localizar,
                    paste0("./promociones/", nombre_archivo, ".xlsx")) 

# write.table(promociones_a_localizar,
#             file = "./promociones_a_localizar.txt",
#             sep = "\t", 
#             row.names = FALSE,
#             col.names = TRUE)
# 
# write.csv(promociones_a_localizar, file = "./promociones_a_localizar.csv", row.names = FALSE)
