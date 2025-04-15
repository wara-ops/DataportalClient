using PyCall
using DataFrames
using CSV

token = ""

client = pyimport("dataportal").DataportalClient(token)
client.fromDataset("ERDClogs")

pdf = client.getData(35410, datatype="logs")
df = CSV.File(IOBuffer(pdf.to_csv())) |> DataFrame

print(df)