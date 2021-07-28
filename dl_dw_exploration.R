# PIB y Inflacion subyacente

library(tidyverse)



library(sparklyr)
sc <- spark_connect(master = "local")


spark_read_csv(sc, "data-csv/")

url <- "https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/CNTR4893?nult=1000"
spark_read_json(sc, name = "pib", path = url, overwrite = TRUE)



library(rjson)

pib <- readLines(url)
pib <- paste(pib, collapse = " ")
pib <- fromJSON(pib)
write.csv(bind_rows(pib$Data), file = here::here("data_lake", "pib", "pib_0"))

# Read a CSV file into a Spark DataFrame
spark_read_csv(sc, "pib", file.path("file://",
                                    here::here("data_lake", "pib", "pib_0")))

