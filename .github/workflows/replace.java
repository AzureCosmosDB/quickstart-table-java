connectionString = os.getenv("CONFIGURATION__AZURECOSMOSDB__CONNECTIONSTRING")
if not connectionString:
    raise EnvironmentError("Azure Cosmos DB for Table connection string not set.")

client = TableServiceClient(connectionString=connectionString)