# COMTRADE

### Description
A set of code to pre-process the COMTRADE `.csv` files and optionally create a series of `h5 or hdf5` binary typed files for machines with limited memory. These are typically 9 times smaller than the original files.

A `combined_comtrade.hdf5` may also be produced. This is XXX MB and contains all the current data. For comparison the largest individual csv is ...

## Problems fixed
When being saved, some elements within the csv contain lists within the string. This results in the following pattern `"item1,item2, etc.."`. In a comma separated file this gets broken up into many columns, failing to be read by most of the default libraries.

Although it is possible to define a more complex delimiter, the simplest solution lies in repacing the commas within these lists with semi-colons, and preserving the original csv capabilities.

## Prerequisites

### Install AWS cli
```
 pip install awscli --upgrade --user

 aws configure

 <enter your login credentials when prompted>
```
### Install the following python packages
- the basic conda distribution - this contains os,sys,re,pandas,numpy,base etc.
- h5py
- boto3


## Running order
1. Get all the keys using `whatfiles.py`
2. Find all the types needed with `get_types.py`
3. Check for any further tweaks `typearrays.json`

4. Run `correct.py` for all required files. This is best done in parallel using the shell script and slurm (a different file may be required for other managers, eg PBS. )

5. Concat all the hdf5 files into a single, smaller one using `concat_h5.py`

6. Extract and query code with an adaption of `extract_full.py`


## Directory contents and their description.
### data/
This is where all the parsed csv files go.  

### baby_viking.slurm & begin_comtrade_reparse.sh
A Job array slurm configuration file for parsing each individual year (a perfectly parallel problem). This submits n tasks with m cores each where n corresponds to the number of files that need to be parsed and c is the number of columns within each file.

### check.py
Runs a simple `,` matching check on each line of a file and outputs lines that are not in compliance. This gives a brief overview of which lines are problematic, and what the issues may be.

### concat_h5.py
Joins all the created h5 files to produce `combined_comtrade.hdf5`, which contains all the years, and each column saved as a dataset.

### correct.py
Corrects the problems outlined above and produces the csv and hdf5 files. If run independantly, it reads one argument corresponding to either the file key needed, or an integer referencing this name from `read.files`.

### extractHS.py
Read all the hdf5 files and pull out any data matching a set of two hscodes (in this case refering to feed data)

### extract_full.py
tbw - will do the same as above but using the combined dataset. Use this if possible.

### HS.json
A nested json object allowing us to read the category desciption related to a hs code. Hierarchy: `jsonfile[<category>][<hscode>]`

### get_types.py
Uniformly sample n lines across all the rows in all the files and determine a potential column type. Manual overrrides are included, such to preserve quantities, eg leading zeros in the hs codes. This produces `typearrays.json` which is later used.

### whatfiles.py
Gets all the files in the relevant trace bucket and saves their keys into `read.files`.
