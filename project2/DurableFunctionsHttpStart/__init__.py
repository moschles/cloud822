'''
+ CLIENT +

    Durable functions client.

    Triggers with http POST or GET 
'''
 
import logging
import json
import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    # Modify to retrieve a books list from a POST body.
    requestBody = json.loads(req.get_body().decode())  
    fxnName = req.route_params["functionName"]

    # Forward the POST body to the Orchestrator
    instance_id = await client.start_new(fxnName, client_input=requestBody )

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)