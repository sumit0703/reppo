
using System;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDBService
{
  public static partial class Ddb_Intro
  {
    /*--------------------------------------------------------------------------
     *                             ReadingMovie_async
     *--------------------------------------------------------------------------*/
    public static async Task<bool> ReadingMovie_async( int year, string title, bool report )
    {
      // Create Primitives for the HASH and RANGE portions of the primary key
      Primitive hash = new Primitive(year.ToString(), true);
      Primitive range = new Primitive(title, false);

      operationSucceeded = false;
      operationFailed = false;
      try
      {
        Task<Document> readMovie = moviesTable.GetItemAsync(hash, range, token);
        if( report )
          msg.AppendFormat( "\n  -- Reading the {0} movie \"{1}\" from the Movies table...", year, title );
        movie_record = await readMovie;
        if( movie_record == null )
        {
          if( report )
            msg.AppendFormat( "\n     -- Sorry, that movie isn't in the Movies table." );
          return ( false );
        }
        else
        {
          if( report )
            msg.AppendFormat( "\n     -- Found it!  The movie record looks like this:\n{0}" ,
                                movie_record.ToJsonPretty() );
          operationSucceeded = true;
          return ( true );
        }
      }
      catch( Exception ex )
      {
        msg.AppendFormat( "  \n   FAILED to get the movie, because: {0}.", ex.Message );
        operationFailed = true;
      }
      return ( false );
    }
  }
}