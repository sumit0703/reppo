using System;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDBService
{
  public static partial class Ddb_Intro
  {
    /*--------------------------------------------------------------------------
     *     WritingNewMovie
     *--------------------------------------------------------------------------*/
    public static async Task WritingNewMovie_async( Document newItem )
    {
      operationSucceeded = false;
      operationFailed = false;

      int year = (int) newItem["year"];
      string name = newItem["title"];

      if( await ReadingMovie_async( year, name, false ) )
        msg.AppendFormat( "\n  The {0} movie \"{1}\" is already in the Movies table...\n" +
                           "  -- No need to add it again... its info is as follows:\n{2}",
                           year, name, movie_record.ToJsonPretty( ) );
      else
      {
        try
        {
          Task<Document> writeNew = moviesTable.PutItemAsync(newItem, token);
          msg.AppendFormat("\n  -- Writing a new movie to the Movies table...");
          await writeNew;
          msg.AppendFormat(" \n     -- Wrote the item successfully!");
          operationSucceeded = true;
        }
        catch (Exception ex)
        {
          msg.AppendFormat(" \n     FAILED to write the new movie, because:\n       {0}.", ex.Message);
          operationFailed = true;
        }
      }
    }
  }
}