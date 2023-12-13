# CARGA DE DATOS DE PROMOCIONES A LOCALIZAR (georreferenciar)
#

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

municipios_2022 <- municipios_2022 %>% 
  mutate(municipio = sub("^EL ", "", municipio),
         municipio = sub("^LA ", "", municipio),
         municipio = sub("^LOS ", "", municipio),
         municipio = sub("^LAS ", "", municipio),
         municipio = sub("\\(CAPITAL\\)$", "", municipio),
         municipio = trimws(municipio))

# Cargo la tabla con las promociones a localizar

archivo <- "../listados 15112022/Listado Sevilla 15-11 - incorporar datos por la Delegación.ods"
promociones_a_localizar <- import(archivo, skip = 1)

promociones_a_localizar <- promociones_a_localizar %>%
  mutate_all(~ifelse(. == "(vacío)", NA, .))

# Adapto los nombres a lo que necesito para  crear la capa de promociones localizadas
# y creo campos vacios para incluir información de la localización (en Qgis)
# 
promociones_a_localizar <- promociones_a_localizar %>% 
  clean_names() %>% 
  mutate(prom_uso = paste(regimen, uso_principal, uso_detallado)) %>% 
  rename(
    prom_exp_po = codigo,
    prom_f_calif_prov = f_provisional,
    prom_f_calif_def = f_definitiva,
    prom_nif_prom = cod,
    prom_denom_prom = nombre_o_razon_social_del_promotor,
    prom_direccion = direccion,
    prom_municipio = municipio,
    prom_nviviendas = total
    ) %>% 
  mutate(
    asig_refcat_d = as.character(NA),
    asig_nviv_localizadas = NA,
    asig_verificado = as.character(NA)
  )

promociones_a_localizar <- promociones_a_localizar %>% 
  mutate(municipio_enlace = sub("\\(EL\\)$", "", prom_municipio),
         municipio_enlace = sub("\\(LA\\)$", "", municipio_enlace),
         municipio_enlace = sub("\\(LOS\\)$", "", municipio_enlace),
         municipio_enlace = sub("\\(LAS\\)$", "", municipio_enlace),
         municipio_enlace = trimws(municipio_enlace))


promociones_a_localizar <-  left_join(
  promociones_a_localizar,
  municipios_2022, by = c("municipio_enlace" = "municipio")
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
