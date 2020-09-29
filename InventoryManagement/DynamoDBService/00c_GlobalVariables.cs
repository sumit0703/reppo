using System.Text;
using System.Threading;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBService
{
    public partial class Ddb_Intro
    {
        // Global variables
        public static bool operationSucceeded;
        public static bool operationFailed;
        public static AmazonDynamoDBClient client;
        public static Table moviesTable;
        public static TableDescription moviesTableDescription;
        public static CancellationTokenSource source = new CancellationTokenSource();
        public static CancellationToken token = source.Token;
        public static Document movie_record;
        public static StringBuilder msg = new StringBuilder();

        public static bool pause()
        {
            if (operationFailed)
            {
                msg.AppendFormat("\n     Operation failed...");
            }
            else if (operationSucceeded)
            {
                msg.AppendFormat("\n    Completed that step successfully!");
            }
            return true;
        }

    }
}
