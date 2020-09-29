
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBService
{
  public static partial class Ddb_Intro
  {
    /*--------------------------------------------------------------------------
     *                             UpdatingMovie_async
     *--------------------------------------------------------------------------*/
    public static async Task<bool> UpdatingMovie_async( UpdateItemRequest updateRequest, bool report )
    {
      UpdateItemResponse updateResponse = null;

      operationSucceeded = false;
      operationFailed = false;
      if( report )
      {
        msg.AppendFormat( "\n  -- Trying to update a movie item..." );
        updateRequest.ReturnValues = "ALL_NEW";
      }

      try
      {
        updateResponse = await client.UpdateItemAsync( updateRequest );
        msg.AppendFormat( "\n     -- SUCCEEDED in updating the movie item!" );
      }
      catch( Exception ex )
      {
        msg.AppendFormat( "\n     -- FAILED to update the movie item, because:\n       {0}.", ex.Message );
        if( updateResponse != null )
          msg.AppendFormat( "\n     -- The status code was " + updateResponse.HttpStatusCode.ToString( ) );
        operationFailed = true;return ( false );
      }
      if( report )
      {
        msg.AppendFormat( "\n     Here is the updated movie informtion: \n" );
        msg.AppendLine( movieAttributesToJson( updateResponse.Attributes ) );
      }
      operationSucceeded = true;
      return ( true );
    }
  }
}

