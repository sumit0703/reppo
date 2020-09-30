using System;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDBService
{
  public static partial class Ddb_Intro
  {

    /*--------------------------------------------------------------------------
     *                       DeletingItem_async
     *--------------------------------------------------------------------------*/
    public static async Task<bool> DeletingItem_async( Table table, int year, string title,
                                                       Expression condition=null )
    {
      Document deletedItem = null;
      operationSucceeded = false;
      operationFailed = false;

      // Create Primitives for the HASH and RANGE portions of the primary key
      Primitive hash = new Primitive(year.ToString(), true);
      Primitive range = new Primitive(title, false);
      DeleteItemOperationConfig deleteConfig = new DeleteItemOperationConfig( );
      deleteConfig.ConditionalExpression = condition;
      deleteConfig.ReturnValues = ReturnValues.AllOldAttributes;

      msg.AppendFormat( "\n  -- Trying to delete the {0} movie \"{1}\"...", year, title );
      try
      {
        Task<Document> delItem = table.DeleteItemAsync( hash, range, deleteConfig );
        deletedItem = await delItem;
      }
      catch( Exception ex )
      {
        msg.AppendFormat( " \n    FAILED to delete the movie item, for this reason:\n       {0}\n", ex.Message );
        operationFailed = true;
        return ( false );
      }
      msg.AppendFormat( " \n    -- SUCCEEDED in deleting the movie record that looks like this:\n {0}" ,
                            deletedItem.ToJsonPretty( ) );
      operationSucceeded = true;
      return ( true );
    }
  }
}