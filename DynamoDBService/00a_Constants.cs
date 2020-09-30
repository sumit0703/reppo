using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBService
{
    public partial class Ddb_Intro
    {
        /*==========================================================================
         *      Constant/Static Values Used by this introductory sample
         *==========================================================================*/
        public const string commaSep = ", ";
        public const string stepString =
          "\n\n--------------------------------------------------------------------------------------" +
          "\n    STEP {0}:  {1}" +
          "\n--------------------------------------------------------------------------------------";

        /*---------------------------------------------------------
         *    1.  The data used to create a new table
         *---------------------------------------------------------*/
        // movies_table_name
        public const string movies_table_name = "Movies";

        // key names for the Movies table
        public const string partition_key_name = "year";
        public const string sort_key_name = "title";

        // movie_items_attributes
        public static List<AttributeDefinition> movie_items_attributes
          = new List<AttributeDefinition>
        {
      new AttributeDefinition
      {
        AttributeName = partition_key_name,
        AttributeType = "N"
      },
      new AttributeDefinition
      {
        AttributeName = sort_key_name,
        AttributeType = "S"
      }
        };

        // movies_key_schema
        public static List<KeySchemaElement> movies_key_schema
          = new List<KeySchemaElement>
        {
      new KeySchemaElement
      {
        AttributeName = partition_key_name,
        KeyType = "HASH"
      },
      new KeySchemaElement
      {
        AttributeName = sort_key_name,
        KeyType = "RANGE"
      }
        };

        // movies_table_provisioned_throughput
        public static ProvisionedThroughput movies_table_provisioned_throughput
          = new ProvisionedThroughput(1, 1);


        /*---------------------------------------------------------
         *    2.  The path to the JSON movies data file to load
         *---------------------------------------------------------*/
        public const string movieDataPath = "./moviedata_small.json";

        public const bool isLocalDynamoDB = true;

    }
}
