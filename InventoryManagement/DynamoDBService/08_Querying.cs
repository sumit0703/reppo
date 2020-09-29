using System;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using System.Collections.Generic;

namespace DynamoDBService
{
    public static partial class Ddb_Intro
    {
        /*--------------------------------------------------------------------------
         *                             SearchListing_async
         *--------------------------------------------------------------------------*/
        public static async Task<bool> SearchListing_async(Search search)
        {
            int i = 0;
            List<Document> docList = new List<Document>();

            msg.AppendFormat(" \n        Here are the movies retrieved:\n" +
                               "         --------------------------------------------------------------------------\n");
            Task<List<Document>> getNextBatch;
            operationSucceeded = false;
            operationFailed = false;

            do
            {
                try
                {
                    getNextBatch = search.GetNextSetAsync();
                    docList = await getNextBatch;

                }
                catch (Exception ex)
                {
                    msg.AppendFormat(" \n       FAILED to get the next batch of movies from Search! Reason:\n          " +
                                       ex.Message);
                    operationFailed = true;
                    return (false);
                }

                foreach (Document doc in docList)
                {
                    i++;
                    showMovieDocShort(doc);
                }
            } while (!search.IsDone);
            msg.AppendFormat(" \n    -- Retrieved {0} movies.", i);
            operationSucceeded = true;
            return (true);
        }


        /*--------------------------------------------------------------------------
         *                             ClientQuerying_async
         *--------------------------------------------------------------------------*/
        public static async Task<bool> ClientQuerying_async(QueryRequest qRequest)
        {
            operationSucceeded = false;
            operationFailed = false;

            QueryResponse qResponse;
            try
            {
                Task<QueryResponse> clientQueryTask = client.QueryAsync(qRequest);
                qResponse = await clientQueryTask;
            }
            catch (Exception ex)
            {
                msg.AppendFormat(" \n     The low-level query FAILED, because:\n       {0}.", ex.Message);
                operationFailed = true;
                return (false);
            }
            msg.AppendFormat(" \n    -- The low-level query succeeded, and returned {0} movies!", qResponse.Items.Count);
            if (!pause())
            {
                operationFailed = true;
                return (false);
            }
            msg.AppendFormat(" \n        Here are the movies retrieved:" +
                               " \n --------------------------------------------------------------------------\n");
            foreach (Dictionary<string, AttributeValue> item in qResponse.Items)
                showMovieAttrsShort(item);

            msg.AppendFormat(" \n    -- Retrieved {0} movies.", qResponse.Items.Count);
            operationSucceeded = true;
            return (true);
        }
    }
}