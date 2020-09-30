using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDBService
{
  public static partial class Ddb_Intro
  {
    /*--------------------------------------------------------------------------
     *                             ClientScanning_async
     *--------------------------------------------------------------------------*/
    public static async Task<bool> ClientScanning_async( ScanRequest sRequest )
    {
      operationSucceeded = false;
      operationFailed = false;

      ScanResponse sResponse;
      Task<ScanResponse> clientScan = client.ScanAsync(sRequest);
      try
      {
        sResponse = await clientScan;
      }
      catch( Exception ex )
      {
        msg.AppendFormat( " \n    -- FAILED to retrieve the movies, because:\n        {0}", ex.Message );
        operationFailed = true;
        pause( );
        return( false );
      }
      msg.AppendFormat( " \n    -- The low-level scan succeeded, and returned {0} movies!", sResponse.Items.Count );
      if( !pause( ) )
      {
        operationFailed = true;
        return ( false );
      }

      msg.AppendFormat( " \n        Here are the movies retrieved:\n" +
                         "         --------------------------------------------------------------------------\n" );
      foreach( Dictionary<string, AttributeValue> item in sResponse.Items )
        showMovieAttrsShort( item );

      msg.AppendFormat( " \n    -- Retrieved {0} movies.", sResponse.Items.Count );
      operationSucceeded = true;
      return ( true );
    }
  }
}