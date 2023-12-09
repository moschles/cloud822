'''
+ ACTIVITY +

    Finalout will write the results to an Azure Blob storage.
    "path": "books/testout.txt",

    date:
        Monday, November 27, 2023 5:26:07 PM
'''
import azure.functions as func

def main( payload: str , outputblob: func.Out[str] ) -> str:
    outputblob.set( payload ) 
    return( "{}".format(len(payload)) )

