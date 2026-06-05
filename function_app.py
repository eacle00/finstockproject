import azure.functions as func
import json
import logging
from config import Config
from bronze_processing.processor import BronzeProcessor

app = func.FunctionApp()

@app.function_name(name="processbronze")
@app.route(route="processbronze", methods=["GET", "POST"])
def processbronze(req: func.HttpRequest):
    
    logging.info("Processing stock ingestion request")

    try:

        body = req.get_json()

        mode = body.get("mode", Config.DEFAULT_MODE)

        symbol = body.get("symbol")

        start_date = body.get("start_date", Config.DEFAULT_START_DATE)

        if not symbol:
            return func.HttpResponse(
                "Missing required parameter: symbol",
                status_code=400
            )
        
        processor = BronzeProcessor(symbol=symbol,
                                    mode=mode,
                                    start_date=start_date,
                                    account_url=Config.ACCOUNT_URL,
                                    bronze_data_path=Config.BRONZE_PATH,
                                    bronze_metadata_path=Config.BRONZE_METADATA_PATH
                    )
        
        total_rows = processor.run()

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "mode": mode,
                "rows_processed": total_rows,
                "symbol": symbol
            }),
            mimetype="application/json",
            status_code=200
        )
    
    except Exception as e:

        logging.error(str(e))

        return func.HttpResponse(
            f"Error: {str(e)}",
            status_code=500
        )

@app.function_name(name="processsilver")
@app.route(route="processsilver", methods=["GET", "POST"])
def processsilver(req: func.HttpRequest):

    return "Hello, silver layer is in progress"