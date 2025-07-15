# NEXT PASS

Tool to get the next pass of Sentinel-1, Sentinel-2, Landsat-8 and Landsat-9 satellites.
The Sentinel-1/2 methods are influenced by [s1_collect_info](https://github.com/forrestfwilliams/s1_collect_info)

## Development setup


### Prerequisite installs
1. Download source code:
```bash
git clone https://github.com/OPERA-Cal-Val/next_pass.git
```
2. Install dependencies, either to a new environment:
```bash
mamba env create --file environment.yml
conda activate next_pass
```
or install within your existing env with mamba.
```bash
mamba install -c conda-forge --yes --file requirements.txt
```

### Usage
```Jupyter Notebook
Use "Run_next_pass.ipynb" 
```
```bash
Point (lat/lon pair):
  python next_pass.py -b 34.20 -118.17

Bounding Box (SNWE):
  python next_pass.py -b 34.15 34.25 -118.20 -118.15

KML File:
  python next_pass.py -b /path/to/file.kml

Define a bounding box and send an email with Sentinel-1 results:
  python next_pass.py -b 50 52 -102 -100 -s sentinel-1 --email

Limit the OPERA products search to a subset given as input 
  python next_pass.py -b 29 31 -100 -97 -p DSWX-HLS_V1 DSWX-S1_V1
```
