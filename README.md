# Csv to json
> Simply transform a json to a csv


This tool helps you create a csv from a json. It creates csv's columns using json hierarchy and a separator delimiter.
For instance if the delimiter is *.* and the json is like this:
```
{
    'team':{
        'captain':
        'defend':
        'str' :
    }
}
```
The csv column's will be

```
team_captain, team_defend, team_str
```


## Installation

OS X, Linux & Windows:

```sh
pip install git+https://github.com/Besedo/json-to-csv
```


## Usage example

```sh
usage: Create csv from multiple files containing one json per line.
       [-h] [--path_data_jsonperline PATH_DATA_JSONPERLINE] [--streaming]
       [--sep SEP] [--int_to_float] [--path_output PATH_OUTPUT]

optional arguments:
  -h, --help            show this help message and exit
  --path_data_jsonperline PATH_DATA_JSONPERLINE
                        File or folder of files containing one json per line
  --streaming           Create the csv in a stream way instead of loading
                        every json in memory
  --sep SEP             Separator used to create columns' names
  --int_to_float        Cast int to float
  --path_output PATH_OUTPUT
                        Path output
```sh

Please refer to [here](https://github.com/Besedo/json-to-csv/examples) for examples.



## Meta

Distributed under the Apache license v2.0. See ``LICENSE`` for more information.

[https://github.com/Besedo/json-to-csv](https://github.com/Besedo/json-to-csv)
