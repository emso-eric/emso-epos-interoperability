# EMSO EPOS Interoperability

**EMSO EPOS Interoperability** is a software toolkit designed to enable seamless integration of EMSO (European Multidisciplinary Seafloor and water column Observatory) data into the EPOS (European Plate Observing System) open-source data portal.

This repository provides all necessary tools to retrieve EMSO data, transform it into interoperable formats, and serve it efficiently to EPOS services.

---

## Overview

EMSO data is distributed through the [EMSO ERIC ERDDAP federation](https://erddap.emso.eu), providing access to heterogeneous ocean observatory data. While ERDDAP can return data in multiple formats, its **GeoJSON** output is not  optimal for rendering within the EPOS data platform.

To address this, the project includes a **protocol adapter** that converts ERDDAP GeoJSON into **CovJSON (CoverageJSON)**, a lightweight and efficient standard for spatio-temporal coverage data, widely supported across EPOS visualization components.

---

## Key Features

- Connects to multiple federated **ERDDAP** data servers
- Retrieves EMSO datasets with spatial and temporal metadata
- Converts **ERDDAP GeoJSON** into **CovJSON** format
- Optimized for integration into the **EPOS open data portal**
- Fully open-source and extensible

---

```
Arhictecture:
    ┌─────────────────┐  ┌─────────────┐  ┌────────────────────┐    
    │   EMSO ERDDAP   │  │   GeoJSON   │  │  EPOS OpenSource   │  
    │   Federation    ◄──►     to      ◄──►      DataPortal    ◄── User  
    │                 │  │   CovJSON   │  │                    │    
    └─────────────────┘  └─────────────┘  └────────────────────┘  
```

## Dependencies

* Install [Docker engine](https://docs.docker.com/engine/install/ubuntu/)
* Install [EPOS Open Source](https://github.com/EPOS-ERIC/epos-opensource) data portal
* Install python3 dependencies:
```bash
pip3 install -r requirements.txt
```

## Deployment
1. Launch geo2coverage converter:
```bash
python3 geo2coverage.py
```
2. Create turtle files for EMSO Datasets
```bash
python3 create_ttls.py
``` 
3. Deploy a new instance of EPOS OpenSource with the deploy script: 

```bash
./deploy.sh
``` 

Done! the data portal with EMSO data should be available at http://localhost:3200


### Customization ### 
To change the logo, overwrite the `logo`

docker cp emso-eric-logo.svg ${tag}-data-portal:/opt/epos-gui/assets/img/logo/logo-white.svg
docker cp emso-eric-logo.ico ${tag}-data-portal:/opt/epos-gui/assets/img/favicon/favicon.ico

### Contact info ###

* **author**: Enoc Martínez  
* **organization**: Universitat Politècnica de Catalunya (UPC)    
* **contact**: enoc.martinez@upc.edu  
