
using System;
using System.Threading.Tasks;

namespace DynamoDBService
{
  public static partial class Ddb_Intro
  {
    /*--------------------------------------------------------------------------
     *                DeletingTable_async
     *--------------------------------------------------------------------------*/
    public static async Task<bool> DeletingTable_async( string tableName )
    {
      operationSucceeded = false;
      operationFailed = false;

      msg.AppendFormat( "\n  -- Trying to delete the table named \"{0}\"...", tableName );
      pause( );
      Task tblDelete = client.DeleteTableAsync( tableName );
      try
      {
        await tblDelete;
      }
      catch( Exception ex )
      {
        msg.AppendFormat( " \n    ERROR: Failed to delete the table, because:\n            " + ex.Message );
        operationFailed = true;
        return ( false );
      }
      msg.AppendFormat( " \n    -- Successfully deleted the table!" );
      operationSucceeded = true;
      pause( );
      return ( true );
    }
  }
}