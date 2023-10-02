import azure.functions as func
import logging
from cleanup import execute as cleanup

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="perform_cleanup")
def perform_cleanup(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    langcode = req.params.get('language')
    if not langcode:
        return func.HttpResponse(f"Please provide the language code", status_code=400)

    result = cleanup(langcode)

    return func.HttpResponse(f"cleanup has been executed for {langcode}, result = {result}")
