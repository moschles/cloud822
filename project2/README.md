## Durable functions recipe

```
command palette
	Azure Functions: Create New Project... 
	Python
	Skip Interpreter for now
	Skip for now
	Skip for now

The next two steps will simply run VSCode in a python virtual environment. 

Open Anaconda3 powershell window 
	conda deactivate
	conda activate azurenv
	cd  C:\azureproject\wordfrqybook
	C:\msVScode\bin\code .  

Open a terminal inside VSCode by  CTRL + SHFT + `
cd C:\Users\thinkpad23i7\anaconda3\Scripts 
./conda.exe info
	This should list the "active environment" as azurenv  
	
edit requirements.txt  to read, 
	azure-functions
	azure-functions-durable
	
cd C:\azureproject\wordfrqybook	
python -m pip install -r requirements.txt

Command palette >> Azure Functions: Create Function.. 
	Durable Functions Orchestrator
	"wfrqyOrchestrator" 
	
Command palette >> Azure Functions: Create Function...
	Durable Functions Activity
	"Mapper" 
	
Change code in Mapper to run parallel. 
	import asyncio
	async def main(name: str) -> str:
	
Command palette >> Azure Functions: Create Function...
	Durable Functions HTTP starter  
	"DurableFunctionsHttpStart" 
	Authorization level anonymous 
	
Large Azure 'A' sidebar >>  Resources >> Azure Subscription 1 >> Function App

Right-click >> Create Function App in Azure (Advanced)  
	bronzefrequencyfxnapp
	(select storage and such here) 
	Consumption plan
	Create new Insights >>  bronzefrequencyinsight
	
Resources >> Remote >> (mouse over) >> Click circle arrow.  
	'bronzefrequencyfxnapp'  should appear under  'Function App'
	
Workspace >> Local >> (mouse over) >> (click lightning strike)   
>> Deploy to Function App...

Perform tests in Postman  
	GET  https://bronzefrequencyfxnapp.azurewebsites.net/api/orchestrators/wfrqyOrchestrator
	copy statusQueryGetUri   perform another GET on that. 
	
Large Azure 'A' sidebar >>  Resources >> Azure Subscription 1

>> Storage accounts >> storage88accnt >> (right click) >> Copy connection string.  

Store this string somewhere safe. 

DefaultEndpointsProtocol=https;AccountName=storage88accnt;AccountKey=V5PDy...+/Mxg==;EndpointSuffix=core.windows.net

Explorer (pages icon)  You should see your sourcecode.  

File >> Save Workspace As ..  
	use the default name.  select the top-level directory of your sourcecode. 

Re-check this after reloading various workspaces.
In terminal, 
	cd C:\Users\thinkpad23i7\anaconda3\Scripts\	
	./conda.exe info 
	active environment should be 'azurenv'  
	
Large Azure 'A' sidebar >>  Resources >> Azure Subscription 1
>> Storage accounts >> Blob Containiners >> (right click) >> Create Blob container 
	"books" 

	

Edit  file host.json  to look like this
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    }
  },
  "extensions": {
    "durableTask": {
      "maxConcurrentActivityFunctions": 100,
      "maxConcurrentOrchestratorFunctions": 3
    }
  }, 
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[3.*, 4.0.0)"
  }
}
	
Edit file local.settings.json to look like this

{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
    "BlobStorageConnection": "DefaultEndpointsProtocol=#####"
  }
}

Where ##### is your safe string from earlier. 

In Azure Portal  (web) 

All resources >> bronzefrequencyfxnapp >> Settings >> Configuration 

+ New application setting
	Name = BlobStorageConnection
	Value = DefaultEndpointsProtocol=#####
	
Where ##### is your safe string from earlier. 

Click 3.5 floppy icon "Save".  Make sure to do this. 


Command Palette >> Azure Functions: Create Function... >> Durable Functions activity >> "Finalout" 

Following step creates an output binding to the "books" blob from earlier. 

Under Finalout >> (edit) function.json 

{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "name": "name",
      "type": "activityTrigger",
      "direction": "in"
    },
    {
      "name": "outputblob",
      "type": "blob",
      "dataType": "string",
      "path": "books/testout.txt",
      "connection": "BlobStorageConnection",
      "direction": "out"
    }
  ],
  "disabled": false 
}

Under Finalout edit __init__.py to read, 

import logging
import asyncio
import azure.functions as func
async def main(name: str, outputblob: func.Out[str] ) -> str:
    ofstring = "polish teapot"
    outputblob.set(ofstring) 
    return(name)

This will create a text file containing "polish teapot" in the books blob. 

under  wfrqyOrchestrator ,  edit __init__.py  so that Mapper workers are run in parallel. 

import azure.functions as func
import azure.durable_functions as df
import logging
import json
from time import perf_counter_ns
def orchestrator_function(context: df.DurableOrchestrationContext ):
    logging.info( "[debug] A3" ) 
    tic = perf_counter_ns()    
    parallelFanout = []
    parallelFanout.append(  context.call_activity('Mapper', "Tokyo") )
    parallelFanout.append(  context.call_activity('Mapper', "Chicago") )
    parallelFanout.append(  context.call_activity('Mapper', "Metansas") )
    # Run map workers in parallel and wait for all to complete. 
    mapStageResults = yield context.task_all(parallelFanout)
    toc = perf_counter_ns()
    elapse = (toc-tic) /  (10.0 ** 9)  
    logging.info( "[debug] elapsed {} sec".format(elapse) ) 
    orchRet = []
    for m in mapStageResults :
        orchRet.append(m) 
    return orchRet 

main = df.Orchestrator.create(orchestrator_function)


Now call the Finalout activity only once at the end of the orchestrator.  Add the line.

for m in mapStageResults :
    orchRet.append(m) 
resultf = yield context.call_activity('Finalout', "Poland") 
return orchRet 

Save all files and edits.

Azure 'A' icon >> Workspace >> (mouse over) >> Lightning icon >> Deploy to Function App.. 

Invoke a run of this function app using postman. 

To view live logs of your function app running on Azure.  
	Azure Portal web
	>> bronzefrequencyfxnapp >> Overview >> Name >> wfrqyOrchestrator 
	>> Developer >> Monitor >> Logs 
	
It is possible to view the created file, testout.txt, through Azure Portal via browser but not necessary.

Azure 'A' icon >> Resources >> Azure subscription 1 >> Storage Accounts >> storage88accnt 
>> Blob Containers >> books 

	testout.txt   

This file should occur there. It can be opened directly in VSCode.  Contents should be

	polish teapot 
	
	
```