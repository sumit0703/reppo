using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Microsoft.AspNetCore.Mvc;
using DynamoDBService;
using System.Text;

namespace InventoryManagement.Controllers
{
    public class MovieController : Controller
    {
        public dynamic Index()
        {
            return "Are you ready to have fun??????????";
        }
        public dynamic TestComplete()
        {
            Ddb_Intro.msg = new StringBuilder();
            CreateClient();
            CreateTable();
            LoadData();
            Create();
            Read();
            Update();
            Delete();
            SearchByExpression();
            SearchByQueryFilter();
            SearchByQueryRequest();
            ScanFilter();
            ScanRequest();

          //  Ddb_Intro.pause();

            string ret = Ddb_Intro.msg.ToString();
            Ddb_Intro.msg = new StringBuilder();
            return ret;
        }

        public dynamic CreateClient()
        { //  1.  Create a DynamoDB client connected to a DynamoDB-Local instance

            Ddb_Intro.msg.AppendLine(string.Format(Ddb_Intro.stepString, 1,
               "Create a DynamoDB client connected to a DynamoDB-Local instance"));
            Ddb_Intro.createClient(Ddb_Intro.isLocalDynamoDB);
            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic CreateTable()
        {
            //  2.  Create a table for movie data asynchronously
            Console.WriteLine();

            Ddb_Intro.msg.AppendLine(string.Format(Ddb_Intro.stepString, 2,
            "Create a table for movie data"));
            Ddb_Intro.CreatingTable_async(Ddb_Intro.movies_table_name,
                                       Ddb_Intro.movie_items_attributes,
                                       Ddb_Intro.movies_key_schema,
                                       Ddb_Intro.movies_table_provisioned_throughput).Wait();

            try { Ddb_Intro.moviesTable = Table.LoadTable(Ddb_Intro.client, Ddb_Intro.movies_table_name); }
            catch (Exception ex)
            {
                Ddb_Intro.operationFailed = true;
                Ddb_Intro.msg.AppendFormat(
                  " Error: Could not access the new '{0}' table after creating it;\n" +
                  "        Reason: {1}.", Ddb_Intro.movies_table_name, ex.Message);

            }

            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic LoadData()
        {  //  3.  Load movie data into the Movies table asynchronously

            if ((Ddb_Intro.moviesTableDescription != null) &&
                (Ddb_Intro.moviesTableDescription.ItemCount == 0))
            {
                Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, 3,
                  "Load movie data into the Movies table");
                Ddb_Intro.LoadingData_async(Ddb_Intro.moviesTable, Ddb_Intro.movieDataPath).Wait();

            }
            else
            {
                Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, 3,
                   "Skipped: Movie data is already loaded in the Movies table");

            }

            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic Create()
        {    //  4.  Add a new movie to the Movies table

            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, 4,
       "Add a new movie to the Movies table");
            Document newItemDocument = new Document();
            newItemDocument["year"] = 2018;
            newItemDocument["title"] = "The Big New Movie";
            newItemDocument["info"] = Document.FromJson(
                "{\"plot\" : \"Nothing happens at all.\",\"rating\" : 0}");

            Ddb_Intro.WritingNewMovie_async(newItemDocument).Wait();
            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic Read()
        {
            //  5.  Read and display the new movie record that was just added
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, 5,
              "Read and display the new movie record that was just added");
            Ddb_Intro.ReadingMovie_async(2018, "The Big New Movie", true).Wait();
            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic Update()
        {
            //  6.  Update the new movie record in various ways
            //-------------------------------------------------
            //  6a.  Create an UpdateItemRequest to:
            //       -- modify the plot and rating of the new movie, and
            //       -- add a list of actors to it
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "6a",
              "Change the plot and rating for the new movie and add a list of actors");
            UpdateItemRequest updateRequest = new UpdateItemRequest()
            {
                TableName = Ddb_Intro.movies_table_name,
                Key = new Dictionary<string, AttributeValue>
                {
                  { Ddb_Intro.partition_key_name, new AttributeValue { N = "2018" } },
                  { Ddb_Intro.sort_key_name, new AttributeValue { S = "The Big New Movie" } }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                  { ":r", new AttributeValue { N = "5.5" } },
                  { ":p", new AttributeValue { S = "Everything happens all at once!" } },
                  { ":a", new AttributeValue { L = new List<AttributeValue>
                    { new AttributeValue { S ="Larry" },
                      new AttributeValue { S = "Moe" },
                      new AttributeValue { S = "Curly" } }
                    }
                  }
                },
                UpdateExpression = "SET info.rating = :r, info.plot = :p, info.actors = :a",
                ReturnValues = "NONE"
            };
            Ddb_Intro.UpdatingMovie_async(updateRequest, true).Wait();
            Ddb_Intro.pause();


            //  6b  Change the UpdateItemRequest so as to increment the rating of the
            //      new movie, and then make the update request asynchronously.
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "6b",
              "Increment the new movie's rating atomically");
            Ddb_Intro.msg.AppendFormat(" \n -- Incrementing the rating of the new movie by 1...");
            updateRequest.ExpressionAttributeValues = new Dictionary<string, AttributeValue>
              {
                { ":inc", new AttributeValue { N = "1" } }
              };
            updateRequest.UpdateExpression = "SET info.rating = info.rating + :inc";
            Ddb_Intro.UpdatingMovie_async(updateRequest, true).Wait();
            Ddb_Intro.pause();


            //  6c  Change the UpdateItemRequest so as to increment the rating of the
            //      new movie, and then make the update request asynchronously.
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "6c",
              "Now try the same increment again with a condition that fails... ");
            Ddb_Intro.msg.AppendFormat("\n  -- Now trying to increment the new movie's rating, but this time\n" +
                                "     ONLY ON THE CONDITION THAT the movie has more than 3 actors...");
            updateRequest.ExpressionAttributeValues.Add(":n", new AttributeValue { N = "3" });
            updateRequest.ConditionExpression = "size(info.actors) > :n";
            Ddb_Intro.UpdatingMovie_async(updateRequest, true).Wait();
            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic Delete()
        {
            //  7.  Try conditionally deleting the movie that we added

            //  7a.  Try conditionally deleting the movie that we added
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "7a",
              "Try deleting the new movie record with a condition that fails");
            Ddb_Intro.msg.AppendFormat(" \n -- Trying to delete the new movie,\n" +
                               "     -- but ONLY ON THE CONDITION THAT its rating is 5.0 or less...");
            Expression condition = new Expression();
            condition.ExpressionAttributeValues[":val"] = 5.0;
            condition.ExpressionStatement = "info.rating <= :val";
            Ddb_Intro.DeletingItem_async(Ddb_Intro.moviesTable, 2018, "The Big New Movie", condition).Wait();
            Ddb_Intro.pause();

            //  7b.  Now increase the cutoff to 7.0 and try to delete again...
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "7b",
              "Now increase the cutoff to 7.0 and try to delete the movie again...");
            Ddb_Intro.msg.AppendFormat(" \n -- Now trying to delete the new movie again,\n" +
                               "     -- but this time on the condition that its rating is 7.0 or less...");
            condition.ExpressionAttributeValues[":val"] = 7.0;

            Ddb_Intro.DeletingItem_async(Ddb_Intro.moviesTable, 2018, "The Big New Movie", condition).Wait();


            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic SearchByExpression()
        {
            //  8.  Query the Movies table in 3 different ways
            Search search;

            //  8a. Just query on the year
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "8a",
              "Query the Movies table using a Search object for all movies from 1985");
            Ddb_Intro.msg.AppendFormat("\n  -- First, create a Search object...");
            try
            {
                search = Ddb_Intro.moviesTable.Query(1985, new Expression());
                Ddb_Intro.msg.AppendFormat("\n     -- Successfully created the Search object,\n" +
                             "        so now we'll display the movies retrieved by the query:");
                Ddb_Intro.SearchListing_async(search).Wait();
            }
            catch (Exception ex)
            {
                Ddb_Intro.msg.AppendFormat("\n     ERROR: Failed to create the Search object because:\n            " +
                                   ex.Message);
                Ddb_Intro.pause();

            }

            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic SearchByQueryFilter()
        {
            Search search;
            //  8b. SearchListing_async
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "8b",
            "Query for 1992 movies with titles from B... to Hzz... using Table.Query");
            Ddb_Intro.msg.AppendFormat("\n  -- Now setting up a QueryOperationConfig for the 'Search'...");
            QueryOperationConfig config = new QueryOperationConfig();
            config.Filter = new QueryFilter();
            config.Filter.AddCondition("year", QueryOperator.Equal, new DynamoDBEntry[] { 1992 });
            config.Filter.AddCondition("title", QueryOperator.Between, new DynamoDBEntry[] { "B", "Hzz" });
            config.AttributesToGet = new List<string> { "year", "title", "info" };
            config.Select = SelectValues.SpecificAttributes;
            Ddb_Intro.msg.AppendFormat(" \n    -- Creating the Search object based on the QueryOperationConfig");
            try
            {
                search = Ddb_Intro.moviesTable.Query(config);
                Ddb_Intro.msg.AppendFormat("\n     -- Successfully created the Search object,\n" +
                               "        so now we'll display the movies retrieved by the query.");


                Ddb_Intro.SearchListing_async(search).Wait();
            }
            catch (Exception ex)
            {
                Ddb_Intro.msg.AppendFormat("     ERROR: Failed to create the Search object because:\n            " +
                                   ex.Message);
                Ddb_Intro.pause();
            }

            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic SearchByQueryRequest()
        {
            //  8c. Query using a QueryRequest
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "8c",
              "Query the Movies table for 1992 movies with titles from M... to Tzz...");
            Ddb_Intro.msg.AppendFormat("\n  -- Next use a low-level query to retrieve a selection of movie attributes");
            QueryRequest qRequest = new QueryRequest
            {
                TableName = "Movies",
                ExpressionAttributeNames = new Dictionary<string, string>
            {
              { "#yr", "year" }
            },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
              { ":qYr",   new AttributeValue { N = "1992" } },
              { ":tSt",   new AttributeValue { S = "M" } },
              { ":tEn",   new AttributeValue { S = "Tzz" } }
            },
                KeyConditionExpression = "#yr = :qYr and title between :tSt and :tEn",
                ProjectionExpression = "#yr, title, info.actors[0], info.genres, info.running_time_secs"
            };
            Ddb_Intro.msg.AppendFormat("\n     -- Using a QueryRequest to get the lead actor and genres of\n" +
                               "        1992 movies with titles between 'M...' and 'Tzz...'.");
            Ddb_Intro.ClientQuerying_async(qRequest).Wait();


            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic ScanFilter()
        {
            //  9.  Try scanning the movies table to retrieve movies from several decades
            //  9a. Use Table.Scan with a Search object and a ScanFilter to retrieve movies from the 1950s

            Search search;
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "9a",
              "Scan the Movies table to retrieve all movies from the 1950's");
            ScanFilter filter = new ScanFilter();
            filter.AddCondition("year", ScanOperator.Between, new DynamoDBEntry[] { 1950, 1959 });
            ScanOperationConfig scanConfig = new ScanOperationConfig
            {
                Filter = filter
            };
            Ddb_Intro.msg.AppendFormat("\n     -- Creating a Search object based on a ScanFilter");
            try
            {
                search = Ddb_Intro.moviesTable.Scan(scanConfig);
                Ddb_Intro.msg.AppendFormat("\n     -- Successfully created the Search object");
                Ddb_Intro.SearchListing_async(search).Wait();
            }
            catch (Exception ex)
            {
                Ddb_Intro.msg.AppendFormat("\n     ERROR: Failed to create the Search object because:\n            " +
                                   ex.Message);
            }

            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic ScanRequest()
        {
            //  9b. Use AmazonDynamoDBClient.Scan to retrieve movies from the 1960s
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, "9b",
              "Use a low-level scan to retrieve all movies from the 1960's");
            Ddb_Intro.msg.AppendFormat(" \n    -- Using a ScanRequest to get movies from between 1960 and 1969");
            ScanRequest sRequest = new ScanRequest
            {
                TableName = "Movies",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                  { "#yr", "year" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":y_a", new AttributeValue { N = "1960" } },
                    { ":y_z", new AttributeValue { N = "1969" } },
                },
                FilterExpression = "#yr between :y_a and :y_z",
                ProjectionExpression = "#yr, title, info.actors[0], info.directors, info.running_time_secs"
            };

            Ddb_Intro.ClientScanning_async(sRequest).Wait();

            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }

        public dynamic DeleteTable()
        {
            //  10.  Finally, delete the Movies table and all its contents
            Ddb_Intro.msg.AppendFormat(Ddb_Intro.stepString, 10,
              "Finally, delete the Movies table and all its contents");
            Ddb_Intro.DeletingTable_async(Ddb_Intro.movies_table_name).Wait();


            // End:
            Ddb_Intro.msg.AppendFormat(
              "\n=================================================================================" +
              "\n            This concludes the DynamoDB Getting-Started demo program" +
              "\n=================================================================================");


            Ddb_Intro.pause();
            return Ddb_Intro.msg.ToString();
        }
    }
}
