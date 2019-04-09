import os
import argparse
import json
import pandas as pd
import logging
import sys
from glob import glob
from pandas.io.json import json_normalize
from collections import OrderedDict


def get_args():
    """
        Get arguments of the program

        :return: arguments parsed
    """

    parser = argparse.ArgumentParser(
        "Create csv from multiple files containing one json per line."
    )
    parser.add_argument("--path_data_jsonperline", type=str, help="File or folder of files containing one json per line")
    parser.add_argument("--streaming",  action='store_true', default=False, help="Create the csv in a stream way instead of loading every json in memory")
    parser.add_argument("--sep", default='.', help="Separator used to create columns' names")    
    parser.add_argument("--int_to_float", action='store_true', default=False, help="Cast int to float")    
    parser.add_argument("--path_output", type=str, help="Path output")
    parser.add_argument("--remove_null", action='store_true', default=False, help="Remove null values (kept by default)")
    
    args = parser.parse_args()
    return args


def setup_custom_logger(name):
    """
        Create a custom logger

        :param name: name of the logger
        :return: logger
    """
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler(name + '.txt', mode='w')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger


def _flatten(d, parent_key='', sep='_', int_to_float=False, remove_null=False):
    """
        Flatten a nested dictionary to one leve dictionary (recursive function)

        :param d: dictionary
        :param parent_key: parent_key used to create field name      
        :param sep: separator of nested fields 
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        
        :return: list of jsons flattened
    """

    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        # Keep it as a list but continue to separate nested fields
        if isinstance(v, list):
            my_elems = []
            for w in v:
                my_elems_w = []
                if isinstance(w, dict):
                    my_elems_w.extend(_flatten(w, sep=sep, int_to_float=int_to_float, remove_null=remove_null).items())
                elif isinstance(w, str):
                    my_elems.append('"' + w + '"')
                    continue
                elif w != None:
                    my_elems.append(w)
                    continue
                else:
                    if not remove_null:
                        my_elems.append('null')
                    continue
                # Put in in alphabetical order
                my_elems_w = sorted(my_elems_w, key=lambda tup: tup[0])
                my_elems.append(dict(my_elems_w))

            items.append((new_key, my_elems))      
        elif isinstance(v, dict):
            items.extend(_flatten(v, new_key, sep=sep, int_to_float=int_to_float, remove_null=remove_null).items())
        else:
            if isinstance(v, int) and int_to_float:
                items.append((new_key, float(v)))
            else:
                if v != None:
                    items.append((new_key, v))
    return dict(items)


def _transform_jsons(json_list, sep, int_to_float, remove_null):
    """
        Transform list of jsons by flattening those

        :param json_list: list of jsons
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        
        :return: list of jsons flattened
    """

    # Transform
    new_jsons = [_flatten(j, sep=sep, int_to_float=int_to_float, remove_null=remove_null) for j in json_list]
    return new_jsons


def update_df_list(df_list, json_list, sep, int_to_float, remove_null):
    """
        Update list of dataframes with list of jsons

        :param df_list: list of dataframes
        :param json_list: list of jsons
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        
        :return: list of dataframes udpated
    """
   
    data = _transform_jsons(json_list, sep, int_to_float, remove_null)
    df = pd.DataFrame(data)

    df_list.append(df)

    return df_list


def update_csv(path_csv, json_list, columns, sep, int_to_float, remove_null):
    """
        Append a csv with json list

        :param path_csv: path to csv to append
        :param json_list: list of json files
        :param columns: list of columns to dump (order is important)
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
    """

    data = _transform_jsons(json_list, sep, int_to_float, remove_null)
    df = pd.DataFrame(data)


    # Add columns that are missing with nan
    current_columns = df.columns.tolist()
    missing_columns = [col for col in columns if col not in current_columns]
    for col in missing_columns:
        df[col] = ""

    # Order dataframe by the columns detected
    df = df[columns]
    df.to_csv(path_csv, mode='a', header=False, encoding="utf-8", index=None, quoting=1)

    del df
    return


def update_columns_list(columns_list, json_list, sep, int_to_float, remove_null):
    """
        Update columns list with new json information
        Sometimes jsons do not have the same fields
        Here we make the unions of all the columns

        :param columns_list: list of columns to update
        :param json_list: list of jsons
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        
        :return: list of columns updated
    """
    data = _transform_jsons(json_list, sep, int_to_float, remove_null)
    cols = []
    for js in data:
        cols.extend(js.keys())
    columns_list = list(set(columns_list + cols))

    return columns_list


def read_jsons_chunks(file_object, chunk_size=10000):
    """Lazy function to read a json by chunk.
    Default chunk size: 10k"""
    # Check first element of a file
    # If it is "[", that means we have a json array
    first_line = file_object.readline()
    if first_line[0] == '[':
        while True: 
            # Parse the next real chunk_size lines
            data = []
            for i in range(chunk_size):
                # Here it works with one json, or an array of jsons with one json in each line
                # TODO Make it work with no assumption over json 
                # Remove comma and to the next line
                line = file_object.readline().strip(',\n')
                # If EOF obtained or end of jsonarray send what's left of the data
                if line == "" or line == "]":
                    yield data
                    return
                else:
                    data.append(json.loads(line))
            if not data:
                break
            yield data
    # End of file obtained
    elif file_object.read() == ']':
        return None
    # Otherwise, we have one json in the file
    else:
        yield [json.loads(first_line)]

def get_columns(list_data_paths, sep, logger, int_to_float, remove_null, is_json=False):
    """
        Get the columns created accordingly to a list of files containing json

        :param list_data_paths: list of files containing one json per line
        :param sep: separator to use when creating columns' names
        :param logger: logger (used to print)
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param is_json: if set to true, inputs are considered as valid json
        
        :return: Exhaustive list of columns
    """

    columns_list = []

    j = 0
    chunk_size = 500000
    for data_file in list_data_paths:
        logger.info(data_file)
        json_list = []
        # If we deal with json (or json array) file
        if is_json:
            f = open(data_file)
            # Read json file by chunk
            for x in read_jsons_chunks(f, chunk_size=chunk_size):
                j += 1
                if (j % chunk_size == 0):
                    columns_list = update_columns_list(columns_list, json_list, sep, int_to_float, remove_null)
                    logger.info('Iteration ' + str(j) + ': Updating columns ===> ' + str(len(columns_list)) + ' columns found')                    
                    json_list = []
                try:
                    json_list.extend(x)
                    # Maximum of chunk_size elements were added
                    j+=chunk_size - 1 # -1 because we add 1 at the beginning of the loop
                except:
                    logger.info("Json in line " + str(i) + " (in file: " + data_file + ") does not seem well formed. Example was skipped")
                    continue
        # If we deal with ljson
        else:
            with open(data_file) as f:
                for i, line in enumerate(f):
                    j += 1
                    if (j % 500000 == 0):
                        columns_list = update_columns_list(columns_list, json_list, sep, int_to_float, remove_null)
                        logger.info('Iteration ' + str(j) + ': Updating columns ===> ' + str(len(columns_list)) + ' columns found')                    
                        json_list = []
                    try:
                        json_list.append(json.loads(line))
                    except:
                        logger.info("Json in line " + str(i) + " (in file: " + data_file + ") does not seem well formed. Example was skipped")
                        continue
        # A quicker solution would be to join directly to create a valid json
        if (len(json_list) > 0):
            columns_list = update_columns_list(columns_list, json_list, sep, int_to_float, remove_null)
            logger.info('Iteration ' + str(j) + ': Updating columns ===> ' + str(len(columns_list)) + ' columns found')

    # Concatenate the dataframes created
    logger.info('Full column\'s list obtained: ' + str(len(columns_list)) + ' fields found')
    return columns_list


def get_dataframe(list_data_paths, columns=None, path_csv=None, logger=None, sep='.', int_to_float=False, remove_null=False):
    """
        Get dataframe from files containing one json per line

        :param list_data_paths: list of files containing one json per line
        :param columns_list: list of columns to update        
        :param path_csv: path to csv output if streaming
        :param logger: logger (used to print)
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        
        :return: dataframe or nothing if the dataframe is generated while streaming the files
    """

    json_list = [] 
    j = 0
    for data_file in list_data_paths:
        logger.info(data_file)
        with open(data_file) as f:
            for i, line in enumerate(f):
                j += 1
                if (j % 500000 == 0):
                    logger.info('Iteration ' + str(j) + ': Creating sub dataframe')
                    if columns:
                        update_csv(path_csv, json_list, columns, sep, int_to_float, remove_null)
                        json_list.clear()

                if (j % 100000 == 0):
                    logger.info(str(i) + ' documents processed')
                try:
                    json_list.append(json.loads(line))
                except:
                    logger.info("Json in line " + str(i) + " (in file: " + data_file + ") does not seem well formed. Example was skipped")
                    continue

        # A quicker solution would be to join directly to create a valid json
        logger.info('Convert to DataFrame')
        if (len(json_list) > 0):
            logger.info('Iteration ' + str(j) + ': Creating last sub dataframe')            
            if columns:
                logger.info("updating csv with new data " + path_csv)                
                update_csv(path_csv, json_list, columns, sep, int_to_float, remove_null)
                json_list.clear()                              

    if not columns:
        # Concatenate the dataframes created
        list_of_dfs = update_df_list([], json_list, sep, int_to_float, remove_null)
        logger.info('Concatenate ' + str(len(list_of_dfs)) + ' DataFrames')
        df = pd.concat(list_of_dfs)

        # Sort columns in alphabetical order
        columns_list = list(df.columns.values)
        columns_list.sort()
        
        return df[columns_list]
    else:
        return


def main():
    """
        Main function of the program
    """

    # Load arguments
    opt = get_args()

    logger = setup_custom_logger('json_to_csv_logger')
    assert os.path.exists(opt.path_data_jsonperline)
    try:
        os.makedirs(os.path.dirname(opt.path_output))
    except:
        logger.info("Folder already exists. Overwriting it")
        pass
    if os.path.isdir(opt.path_data_jsonperline):
        logger.info("Reading files in " + opt.path_data_jsonperline)
        data = glob(os.path.join(opt.path_data_jsonperline, '*'))
    else:
        logger.info("Reading " + opt.path_data_jsonperline) 
        data = [opt.path_data_jsonperline]   
    
    # Get list of columns if in streaming
    columns_list = None
    if opt.streaming:
        columns_list = get_columns(data, opt.sep, logger, opt.int_to_float, opt.remove_null)
        # Sort columns in alphabetical order
        columns_list.sort()
        df = pd.DataFrame(columns=columns_list)
        logger.info(columns_list)

        # Dump empty dataframes with columns
        df.to_csv(opt.path_output, encoding="utf-8", index=None, quoting=1)
    
    # Get dataframe
    df = get_dataframe(data, columns=columns_list, path_csv=opt.path_output, logger=logger, sep=opt.sep, int_to_float=opt.int_to_float, remove_null=opt.remove_null)

    if not opt.streaming:
        logger.info("saving data to "  + opt.path_output)
        df.to_csv(opt.path_output, encoding="utf-8", index=None, quoting=1)

    logger.info('Csv successfully created and dumped')
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        logger.info("Uncaught error waiting for scripts to finish")
        logger.info(e)
        raise
