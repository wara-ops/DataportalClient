library(reticulate)
use_virtualenv("/Users/eruujoh/Projects/waraops/dataportalclient/.venv")

dataportal <- import("dataportal")

token <- ""
client <- dataportal$DataportalClient(token)

client$fromDataset('ERDClogs')

df <- client$getData(35410, datatype='logs')

head(df)