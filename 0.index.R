# Contiene varios scripts, dentro de cada uno de los cuales, al inicio hay
# una descripción de lo que hace. No obstante, como resumen
# 
# Creacion_esquema_viv_vp.R
#  Crea el esquema viv_vp copiando dentro la información creada por Jesús Rodríguez.
#  El script no se debe volver a usar salvo que se vuelva a partir de cero
#  
# Creacion_esquema_catastro2022_sgv.R
#  Crea el esquema catastro2022_sgv a partir de la  la capa de parcelas  y de 
#  la tabla de edificios del trabajo del IECA (ambas)
#  El esquema sirve para fabricar la capa de promociones de vivienda protegida 
#  localizadas. El script no se debe volver a usar, salvo para hacerlo de nuevo
#  con información actualizada de año 2023 o posterior
#  
# Crea_promociones_a_localizar.R
#  Toma la tabla de promociones que no están aún georrerenciadas y le añade
#  varios campos que serán precisos para poder grabar en la capa los nuevos registros
#
# calculos_varios.R
#  Carga en memoria los datos de las promociones de vivienda protegida y
#  realiza una serie de cálculos para obtener estadísticas por municipios