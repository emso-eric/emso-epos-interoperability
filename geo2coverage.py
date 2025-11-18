from argparse import ArgumentParser
import yaml
from flask import Flask, request, Response
from flask_cors import CORS
import json
import logging
import erddapy
import pandas as pd
from logging.handlers import TimedRotatingFileHandler
import rich
import requests
import threading
import psutil
import os
import time

app = Flask(__name__)
CORS(app)

def dataframe_to_covjson(df: pd.DataFrame, metadata: dict):
    df = df.drop_duplicates(subset=["time"])
    rich.print(df)
    rich.print(metadata)

    param_names = [p for p in metadata.keys() if p not in ["time", "latitude", "longitude"]]

    lat = df["latitude"].mean()
    lon = df["longitude"].mean()

    parameters = {}
    ranges = {}

    for name in param_names:
        m = metadata[name]
        parameters[name] = {
            "type": "Parameter",
            "description": {"en": m["name"]},
            "unit": {"label": {"en": m["units"]}, "symbol": m["units"]},
            "observedProperty": {
                "id": m["definition"],
                "label": {"en":  m["name"]}
            }

        }

        ranges[name] = {
            "type": "NdArray",
            "dataType": "float",
            "axisNames": ["t"],
            "shape": [len(df)],
            "values": df[name].to_list()
        }

    covjson = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "PointSeries",
            "axes": {
                "t": {"values": df["time"].to_list()},
                "x": {"values": [lon]},
                "y": {"values": [lat]}
            },
            "referencing": [
                {
                    "coordinates": ["x", "y"],
                    "system": {
                        "type": "GeographicCRS",
                        "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
                    }
                },
                {
                    "coordinates": ["t"],
                    "system": {
                        "type": "TemporalRS",
                        "calendar": "Gregorian"
                    }
                }
            ]
        },
        "parameters": parameters,
        "ranges": ranges,
        "location": {
            "type": "Point",
            "coordinates": [lon, lat]
        }
    }

    return covjson


class ErddapDownloader():
    def __init__(self, url, erddap_url):

        self.errdap_url = erddap_url
        self.url = url
        self.e = erddapy.ERDDAP(server=self.errdap_url, protocol='tabledap')
        self.dataset_dict = {}
        self.dataset_dict_t = None
        self.cache_time = 3600

    def get_dataset_dict(self):
        if not self.dataset_dict_t or (time.time() - self.dataset_dict_t) > self.cache_time:
            all_datasets = self.e.get_search_url(response='csv', search_for='all')
            rich.print("Refreshing dataset list")
            df = pd.read_csv(all_datasets)
            dataset_dict = {dataset_id: self.url + f"/{dataset_id}" for dataset_id in df["Dataset ID"].loc[1:]}
            self.dataset_dict = dataset_dict
        return self.dataset_dict

    def get_data(self, dataset_id, params) -> (dict, int):
        url = self.errdap_url + f"/tabledap/{dataset_id}.json" + "?" + params

        rich.print(f"[cyan]Getting data from {url}")
        t = time.time()
        resp = requests.get(url)
        data = resp.json()
        columns = data["table"]["columnNames"]

        rows = data["table"]["rows"]
        data = pd.DataFrame(rows, columns=columns)
        print(data)

        # Now accessing metadata
        url = self.errdap_url + f"/info/{dataset_id}/index.json"
        resp = requests.get(url)
        meta = resp.json()
        columns = meta["table"]["columnNames"]
        rows = meta["table"]["rows"]
        meta = pd.DataFrame(rows, columns=columns)
        get_data_msecs = 1000*(time.time() - t)


        names_and_units = {}

        def get_value_from_meta(df, varname, attr_name, alternative=""):
            df = df[df["Variable Name"] == varname]
            row = df[df["Attribute Name"] == attr_name]
            if row.empty:
                return str(alternative)
            else:
                return row["Value"].values[0]

        for c in data.columns:
            definition = get_value_from_meta(meta, c, "sdn_parameter_urn", alternative="")
            if definition:
                _, vocab, _, term = definition.split(":")
                definition = f"http://vocab.nerc.ac.uk/collection/{vocab}/current/{term}/"

            names_and_units[c] = {
                "name": get_value_from_meta(meta, c, "standard_name", alternative=c),
                "units": get_value_from_meta(meta, c, "units"),
                "definition": definition
            }

        if resp.status_code > 399:
            rich.print(f"[red]HTTP CODE: {resp.status_code}")
            rich.print(f"[red]HTTP ERROR: {resp.text}")
            return {"error": resp.text}, resp.status_code
        t = time.time()
        data = dataframe_to_covjson(data, names_and_units)
        rich.print(f"[cyan]   getting data took {get_data_msecs:.02f} msecs")
        rich.print(f"[cyan]   Formatting response response {1000 * (time.time() - t):.02f} msecs")
        return data, 200

def setup_log(name, path="log", log_level="debug"):
    """
    Setups the logging module
    :param name: log name (.log will be appended)
    :param path: where the logs will be stored
    :param log_level: log level as string, it can be "debug, "info", "warning" and "error"
    """

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Check arguments
    if len(name) < 1 or len(path) < 1:
        raise ValueError("name \"%s\" not valid", name)
    elif len(path) < 1:
        raise ValueError("name \"%s\" not valid", name)

    # Convert to logging level
    if log_level == 'debug':
        level = logging.DEBUG
    elif log_level == 'info':
        level = logging.INFO
    elif log_level == 'warning':
        level = logging.WARNING
    elif log_level == 'error':
        level = logging.ERROR
    else:
        raise ValueError("log level \"%s\" not valid" % log_level)

    if not os.path.exists(path):
        os.makedirs(path)

    filename = os.path.join(path, name)
    if not filename.endswith(".log"):
        filename += ".log"
    print("Creating log", filename)
    print("name", name)

    logger = logging.getLogger()
    logger.setLevel(level)
    log_formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)-7s: %(message)s',
                                      datefmt='%Y/%m/%d %H:%M:%S')
    handler = TimedRotatingFileHandler(filename, when="midnight", interval=1, backupCount=7)
    handler.setFormatter(log_formatter)
    logger.addHandler(handler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(log_formatter)
    logger.addHandler(consoleHandler)

    logger.info("")
    logger.info(f"===== {name} =====")

    return logger

@app.route('/geo2coverage/v1.0/help', methods=['GET'])
def geo2coverage_help():
    document = {
        "message": "it works!"
    }
    return Response(json.dumps(document), status=200, mimetype="application/json")

@app.route('/geo2coverage/v1.0/datasets', methods=['GET'])
def geo2coverage_datasets():
    document = app.erddap.get_dataset_dict()
    return Response(json.dumps(document), status=200, mimetype="application/json")

@app.route('/geo2coverage/v1.0/<dataset_id>', methods=['GET'])
def geo2coverage_data(dataset_id):
    rich.print(f"Trying to fetch data from {dataset_id}")
    rich.print(f"original url: {request.url}")
    if "?" not in request.url:
        opts = ""
    else:
        opts = request.url.split("?")[1]
    data, code = app.erddap.get_data(dataset_id, params=opts)
    return Response(json.dumps(data), status=code, mimetype="application/json")

# 3. Function to list all endpoints
def list_endpoints():
    output = []
    # app.url_map contains all URL rules registered with the application
    for rule in app.url_map.iter_rules():
        # Filter out built-in Flask routes (like 'static')
        if rule.endpoint != 'static':
            methods = ','.join(rule.methods - set(['HEAD', 'OPTIONS']))
            output.append({
                'endpoint': rule.endpoint,
                'methods': methods,
                'path': str(rule)
            })

    # Sort the list by path for better readability
    output.sort(key=lambda item: item['path'])

    print("--- Available API Endpoints ---")
    for item in output:
        # print the results
        print(f"Endpoint: {item['endpoint']:<15} | Methods: {item['methods']:<10} | Path: {item['path']}")
    print("-------------------------------")

def show_usage():
    process = psutil.Process(os.getpid())
    while True:
        cpu = process.cpu_percent(interval=1)
        mem = process.memory_info().rss / (1024 ** 2)  # MB
        rich.print(f"[cyan] CPU: {cpu}% | RAM: {mem:.2f} MB")
        time.sleep(1)


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-e", "--erddap-url", help="ERDDAP URL to download data", type=str, default="https://erddap.emso.eu/erddap")
    argparser.add_argument("-url", "--url", help="host", type=str, default="http://localhost:5000/geo2coverage/v1.0")
    argparser.add_argument("-p", "--port", help="port", type=int, default=5000)

    args = argparser.parse_args()

    log = setup_log("Setting UP API")
    t = threading.Thread(target=show_usage, daemon=True)
    t.start()

    erddap = ErddapDownloader(args.url, args.erddap_url)
    app.erddap = erddap
    list_endpoints()
    app.run(host="0.0.0.0", port=args.port, debug=False)
