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
python next_pass.py -b 34 35 -119 -117 --sat sentinel-1
python next_pass.py -b 34 35 -119 -117 --sat sentinel-2 -fp'../../data/KML/LA_National_Forest.kml'
python next_pass.py -b 34 34 -119 -119 --sat landsat
```
