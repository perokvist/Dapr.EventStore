using Microsoft.Azure.Cosmos;
using System.Threading.Tasks;

namespace Dapr.EventStore.Tests;
public class CosmosUtil
{
    const string connectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    static CosmosClient cosmosClient = new(connectionString);

    public static async Task DeleteAsync(string dataBaseName, string containerName)
    {
        var container = cosmosClient.GetContainer(dataBaseName, containerName);
        await container.DeleteContainerAsync();
    }

    public static async Task CreateAsync(string dataBaseName, string containerName)
    {
        var db = await cosmosClient.CreateDatabaseIfNotExistsAsync(dataBaseName);
        await db.Database.CreateContainerIfNotExistsAsync(new()
        {
            Id = containerName,
            PartitionKeyPath = "/partitionKey"
        });
    }

}
