from argparse import ArgumentParser
from email.policy import default

import rich
import os
import requests
import pandas as pd


def get_erddap_metadata(erddap_url: str) -> pd.DataFrame:
    """
    Downloads metadata for all datasets from an ERDDAP server and returns a pandas DataFrame.

    Parameters
    ----------
    erddap_url : str
        The base URL of the ERDDAP server (e.g., "https://coastwatch.pfeg.noaa.gov/erddap")

    Returns
    -------
    pd.DataFrame
        A DataFrame containing metadata for all datasets available on the ERDDAP server.
    """
    # Ensure URL does not end with a slash
    erddap_url = erddap_url.rstrip("/")

    # ERDDAP provides a CSV table of all datasets at this endpoint
    metadata_url = f"{erddap_url}/info/index.csv"

    try:
        response = requests.get(metadata_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch metadata from ERDDAP: {e}")

    # Read the CSV into a DataFrame
    with open("index.csv", "w") as f:
        f.write(response.text)
    df = pd.read_csv("index.csv")
    return df


def get_dataset_metadata(erddap_url: str, dataset_id) -> pd.DataFrame:
    """
    Downloads metadata for all datasets from an ERDDAP server and returns a pandas DataFrame.

    Parameters
    ----------
    erddap_url : str
        The base URL of the ERDDAP server (e.g., "https://coastwatch.pfeg.noaa.gov/erddap")

    Returns
    -------
    pd.DataFrame
        A DataFrame containing metadata for all datasets available on the ERDDAP server.
    """
    # Ensure URL does not end with a slash
    erddap_url = erddap_url.rstrip("/")

    # ERDDAP provides a CSV table of all datasets at this endpoint
    metadata_url = f"{erddap_url}/info/{dataset_id}/index.csv"

    try:
        response = requests.get(metadata_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch metadata from ERDDAP: {e}")

    # Read the CSV into a DataFrame
    with open("index.csv", "w") as f:
        f.write(response.text)
    df = pd.read_csv("index.csv")
    return df


def ttl_from_erddap(df, dataset_id, converter_url, folder):
    ttl_file = os.path.join(folder, dataset_id  + ".ttl")
    institution = df.loc[df["Attribute Name"] == "institution"]["Value"].values[0]
    title = df.loc[df["Attribute Name"] == "title"]["Value"].values[0]
    description = df.loc[df["Attribute Name"] == "summary"]["Value"].values[0]
    license_uri = df.loc[df["Attribute Name"] == "license_uri"]["Value"].values[0]
    keywords = df.loc[df["Attribute Name"] == "keywords"]["Value"].values[0]
    start_time = df.loc[df["Attribute Name"] == "time_coverage_start"]["Value"].values[0]
    lat = df.loc[df["Attribute Name"] == "geospatial_lat_max"]["Value"].values[0]
    lon = df.loc[df["Attribute Name"] == "geospatial_lon_max"]["Value"].values[0]

    if not title:
        rich.print(f"[red]No title for {dataset_id}")
        rich.print(f"[red]No description for {description}")
        return

    now = pd.Timestamp.now(tz="utc").strftime("%Y-%m-%dT%H:%M:%SZ")

    dft = df.loc[df["Variable Name"] == "time"]
    time_range = dft[dft["Attribute Name"] == "actual_range"]["Value"].values[0]
    tmin_erddap, tmax_erddap = time_range.split(", ")

    # Get a list of variables with standard name, discard all QCs
    variables = df.loc[df["Attribute Name"] == "standard_name"]["Variable Name"].unique()
    variables = [v for v in variables if not v.endswith("_QC")]
    variable_str = ",".join(variables)



    def erddap_time_to_timestamp(time_str):
        """ Converts something like 1.2436218E9 into a proper timestamp"""
        if time_str.lower() == "nan":
            return pd.Timestamp.now(tz="utc")        
        number, exponent = time_str.split("E")
        epoch = float(number) * 10**(float(exponent))
        return pd.to_datetime(epoch, unit="s")

    tmin_t = erddap_time_to_timestamp(tmin_erddap)
    tmax_t = erddap_time_to_timestamp(tmax_erddap)

    tmin_str = tmin_t.strftime("%Y-%m-%dT%H:%M:%SZ")
    tmax_str = tmax_t.strftime("%Y-%m-%dT%H:%M:%SZ")
    default_min = (tmax_t - pd.to_timedelta("60d")).strftime("%Y-%m-%dT%H:%M:%SZ")
    default_max = tmax_t.strftime("%Y-%m-%dT%H:%M:%SZ")

    query_url = f"{converter_url}/{dataset_id}?{variable_str}" + "{&time<,time>}"


    contents = f"""
@prefix adms: <http://www.w3.org/ns/adms#> .
@prefix dash: <http://datashapes.org/dash#> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix epos: <https://www.epos-eu.org/epos-dcat-ap#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix cnt: <http://www.w3.org/2011/content#> .
@prefix oa: <http://www.w3.org/ns/oa#> .
@prefix org: <http://www.w3.org/ns/org#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix schema: <http://schema.org/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix spdx: <http://spdx.org/rdf/terms#> .
@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
@prefix hydra: <http://www.w3.org/ns/hydra/core#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> .
@prefix http: <http://www.w3.org/2006/http#> .
@prefix locn: <http://www.w3.org/ns/locn#> .
@prefix gsp: <http://www.opengis.net/ont/geosparql#> .
@prefix dqv: <http://www.w3.org/ns/dqv#> .

#---ContactPoint---
#contact not found

#---Organization---
<{dataset_id}_organization> a schema:Organization;
  schema:identifier [ a schema:PropertyValue;
   schema:propertyID "generic";
   schema:value "{dataset_id}_organization";
  ];
  schema:legalName "{institution}" ;
#institution reference not found
 .

#---Dataset---
<{dataset_id}_COVJSON> a dcat:Dataset;
  dct:identifier "{dataset_id}_COVJSON";
  dct:created "{now}"^^xsd:date;
  dct:modified "{now}"^^xsd:date;
  dct:publisher <{dataset_id}_organization>;
  dcat:keyword "{dataset_id}";
  dct:description "{description}";
  dct:temporal [ a dct:PeriodOfTime;
   schema:startDate "{tmin_str}"^^xsd:dateTime;
  ];
  dct:spatial [ a dct:Location;
   locn:geometry "POINT({lon} {lat})"^^gsp:wktLiteral;
  ];
  dct:title "{title}";
  dcat:theme  <category:EMSO_conc> ;
  dct:type "http://purl.org/dc/dcmitype/Collection"^^xsd:anyURI ;
  dcat:distribution <{dataset_id}_distribution_COVJSON>;
#doi not found
#update interval not found
#time coverage end not found
#quality control methods not found
  .

#---Distribution---
<{dataset_id}_distribution_COVJSON> a dcat:Distribution;
  dct:identifier "{dataset_id}_distribution_COVJSON";
  dct:issued "{now}"^^xsd:date;
  dct:modified "{now}"^^xsd:date;
  dct:license "https://spdx.org/licenses/CC-BY-4.0"^^xsd:anyURI;
  dct:description "{description}";
  dct:title "{title}";
  dct:type "http://publications.europa.eu/resource/authority/distribution-type/WEB_SERVICE"^^xsd:anyURI;
  dct:conformsTo <{dataset_id}_webservice_COVJSON>;
  dcat:accessURL <{dataset_id}_operation_COVJSON> ;
.

#---WebService---
<{dataset_id}_webservice_COVJSON> a epos:WebService;
  schema:identifier "{dataset_id}_webservice_COVJSON";
  schema:datePublished "{now}"^^xsd:date;
  schema:dateModified "{now}"^^xsd:date;
  schema:provider <{dataset_id}_organization>;
  schema:keywords "{keywords}";
  dct:license "{license_uri}"^^xsd:anyURI;
  dct:temporal [ a dct:PeriodOfTime;
   schema:startDate "{start_time}"^^xsd:dateTime;
 ];
  schema:name "{title}";
  schema:description "{description}";
  hydra:supportedOperation <{dataset_id}_operation_COVJSON> ;
  dct:conformsTo <{dataset_id}_APIDocumentation_COVJSON> ;
.

#---ApiDocumentation---
<{dataset_id}_APIDocumentation_COVJSON> a hydra:ApiDocumentation;
  hydra:title "web service documentation" ;
      hydra:description "Brief description of the ISGI web service" ;
  hydra:entrypoint "https://erddap.emso.eu/erddap/rest.html"^^xsd:anyURI;
.

#---Operation---
<{dataset_id}_operation_COVJSON> a hydra:Operation;
  hydra:method "GET"^^xsd:string;
  hydra:returns "covjson";
  hydra:property[ a hydra:IriTemplate;
   hydra:template "{query_url}"^^xsd:string;
       hydra:mapping[ a hydra:IriTemplateMapping;
          hydra:variable "time<"^^xsd:string;
          hydra:property "schema:endDate";
          schema:valuePattern "YYYY-MM-DDThh:mm:ssZ";
          rdfs:range "xsd:dateTime";
          rdfs:label "End time";
          schema:maxValue "{tmax_str}";
          schema:defaultValue "{default_max}";
        ];

       hydra:mapping[ a hydra:IriTemplateMapping;
          hydra:variable "time>"^^xsd:string;
          hydra:property "schema:startDate";
          schema:valuePattern "YYYY-MM-DDThh:mm:ssZ";
          rdfs:range "xsd:dateTime";
          rdfs:label "Start time";
          schema:minValue "{tmin_str}";
          schema:defaultValue "{default_min}";
        ];
        
        #---- ADD VARIABLES HERE
        
    ];
.
    """

    with open(ttl_file, "w") as f:
        f.write(contents)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--url", help="converter url", default="http://172.17.0.1:5000/geo2coverage/v1.0")
    parser.add_argument("--limit", help="limit", default=1000, type=int)
    parser.add_argument("--output", help="Output folder", type=str, default="conf")
    args = parser.parse_args()


    url = "https://erddap.emso.eu/erddap"
    datasets = get_erddap_metadata(url)

    processed = 0
    os.makedirs(args.output, exist_ok=True)

    for _, row in datasets.iterrows():
        dataset_id = row["Dataset ID"]

        if pd.isnull(row["tabledap"]) or dataset_id == "allDatasets":
            rich.print(f"[grey42]Skipping {dataset_id}, not tabledap")
            continue

        rich.print(f"Processing dataset [blue]'{dataset_id}'[/blue] ...", end="")
        df = get_dataset_metadata(url, dataset_id)

        try:
            ttl_from_erddap(df, dataset_id, args.url, args.output)
            rich.print(f"[green]success")
        except IndexError or ValueError or KeyError as e:
            rich.print(f"[red]Error processing {dataset_id}: {e.__repr__()}")

        processed += 1
        if processed > args.limit:
            exit()
            