import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="processbronze")
@app.route(route="processbronze", methods=["GET", "POST"])
def processbronze(req: func.HttpRequest):

    return 

@app.function_name(name="processsilver")
@app.route(route="processsilver", methods=["GET", "POST"])
def processsilver(req: func.HttpRequest):

    return 