import logging
import json
import azure.functions as func
import pandas as pd
import yfinance as yf
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from io import StringIO

def upload_to_adls(df: pd.DataFrame):
    file_system = "stocks"
    account_url = "https://finstocksdata.dfs.core.windows.net/"
    file_name = "data.csv"
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=file_system)

    for (ticker, year, month), group in df.groupby(['Ticker', 'Year', 'Month']):
        buffer = StringIO()
        group.to_csv(buffer, index=False)
        buffer.seek(0)
        directory = f'bronze/{ticker}/{year}/{month:02d}/{file_name}'

        file_client = file_system_client.get_file_client(directory)
        file_client.upload_data(buffer.getvalue(), overwrite=True)
    
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing stock ingestion request")

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)
    
    try:
        mode = body.get("mode", "incremental")
        symbol = body.get("symbol")
        
        if mode == 'initial':
            start_date = body.get("start_date")
            if not all([symbol, start_date]):
                return func.HttpResponse(
                    "Missing required parameters: symbol, start_date",
                    status_code=400
                    )
            else:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
                df = yf.download(tickers=symbol, start=start_date, interval="1d", group_by="column")
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df.reset_index(inplace=True)
                df["Date"] = pd.to_datetime(df['Date'])
                df['Year'] = df['Date'].dt.year
                df['Month'] = df['Date'].dt.month
                df["Ticker"] = symbol

        else:
            if not symbol:
                    return func.HttpResponse(
                    "Missing required parameters: symbol",
                    status_code=400
                    )
            else:
                return func.HttpResponse(
                        "Incremental is not yet available",
                        status_code=400
                        )
            
        
        upload_to_adls(df)

        return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "rows": len(df)
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