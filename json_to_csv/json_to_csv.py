import os
import argparse
import json
import pandas as pd
import logging
import sys
from glob import glob


def get_args():
    """
        Get arguments of the program

        :return: arguments parsed
    """

    parser = argparse.ArgumentParser(
        "Create csv from multiple files containing one json per line."
    )
    parser.add_argument("--path_data_jsonperline", type=str, help="File or folder of files containing one json per line")
    parser.add_argument("--streaming",  action='store_true', default=False, help="Create the csv in a stream way instead of loading every json in memory (default False)")
    parser.add_argument("--sep", default='.', help="Separator used to create columns' names")
    parser.add_argument("--int_to_float", action='store_true', default=False, help="Cast int to float (default False)")
    parser.add_argument("--path_output", type=str, help="Path output")
    parser.add_argument("--remove_null", action='store_true', default=False, help="Remove null values (default False)")
    parser.add_argument("--is_json", action='store_true', default=False, help="Indicate if input file is a json (default False)")
    parser.add_argument("--flatten_list", action='store_true', default=False, help="If true, flatten list of objects (default False)")

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


def _flatten(d, parent_key='', sep='_', int_to_float=False, remove_null=False, flatten_list=False):
    """
        Flatten a nested dictionary to one leve dictionary (recursive function)

        :param d: dictionary
        :param parent_key: parent_key used to create field name
        :param sep: separator of nested fields
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param flatten_list: if set to true, will flatten the content of a list of objects

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
                    my_elems_w.extend(_flatten(w, sep=sep, int_to_float=int_to_float, remove_null=remove_null, flatten_list=flatten_list).items())
                elif isinstance(w, str):
                    my_elems.append(w)
                    continue
                elif w is not None:
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
            items.extend(_flatten(v, new_key, sep=sep, int_to_float=int_to_float, remove_null=remove_null, flatten_list=flatten_list).items())
        else:
            if isinstance(v, int) and int_to_float:
                items.append((new_key, float(v)))
            else:
                if v is not None:
                    items.append((new_key, v))
    return dict(items)


def _transform_jsons(json_list, sep, int_to_float, remove_null, flatten_list):
    """
        Transform list of jsons by flattening those

        :param json_list: list of jsons
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param flatten_list: if set to true, will flatten the content of a list of objects

        :return: list of jsons flattened
    """

    # Transform
    new_jsons = [_flatten(j, sep=sep, int_to_float=int_to_float, remove_null=remove_null, flatten_list=flatten_list) for j in json_list]
    return new_jsons


def update_df_list(df_list, json_list, sep, int_to_float, remove_null, flatten_list):
    """
        Update list of dataframes with list of jsons

        :param df_list: list of dataframes
        :param json_list: list of jsons
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param flatten_list: if set to true, will flatten the content of a list of objects

        :return: list of dataframes udpated
    """

    data = _transform_jsons(json_list, sep, int_to_float, remove_null, flatten_list)
    df = pd.DataFrame(data)

    df_list.append(df)

    return df_list


def update_csv(path_csv, json_list, columns, sep, int_to_float, remove_null, flatten_list):
    """
        Append a csv with json list

        :param path_csv: path to csv to append
        :param json_list: list of json files
        :param columns: list of columns to dump (order is important)
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param flatten_list: if set to true, will flatten the content of a list of objects
    """

    data = _transform_jsons(json_list, sep, int_to_float, remove_null, flatten_list)
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


def update_columns_list(columns_list, json_list, sep, int_to_float, remove_null, flatten_list):
    """
        Update columns list with new json information
        Sometimes jsons do not have the same fields
        Here we make the unions of all the columns

        :param columns_list: list of columns to update
        :param json_list: list of jsons
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param flatten_list: if set to true, will flatten the content of a list of objects

        :return: list of columns updated
    """
    data = _transform_jsons(json_list, sep, int_to_float, remove_null, flatten_list)
    cols = []
    for js in data:
        cols.extend(js.keys())
    columns_list = list(set(columns_list + cols))

    return columns_list


def read_jsons_chunks(file_object, chunk_size=10000):
    """Lazy function to read a json by chunk.
    Default chunk size: 10k"""

    # Parse the next real chunk_size lines
    chunk = file_object.read(1000000)
    data = []
    i = 0
    nb_bracket = 0
    nb_quotes = 0
    example = ""
    count_escape_char = 0
    while True:
        # Read cahracter by character
        for k, c in enumerate(chunk):
            # Check quoting
            if c == '"':
                # Check only when '"' is a delimiter of field or value in json
                if count_escape_char % 2 == 0:
                    nb_quotes += 1
            # Check beginning of brackets
            elif c == '{' and nb_quotes % 2 == 0:
                # Check only when '{' is a delimiter of field or value in json
                if count_escape_char % 2 == 0:
                    nb_bracket += 1
            # Check ending of brackets
            elif c == '}' and nb_quotes % 2 == 0:
                # Check only when '"' is a delimiter of field or value in json
                if count_escape_char % 2 == 0:
                    nb_bracket -= 1
                # This means we finished to read one json
                if nb_bracket == 0 and nb_quotes % 2 == 0:
                    example += c
                    data.append(json.loads(example))
                    i += 1
                    # When chunk_size jsons obtained, dump those
                    if i % chunk_size == 0:
                        yield(data)
                        data = []

                    # Initialize those
                    example = ""
                    continue
            # If we are in between 2 json examples or at the beginning
            elif c in ['[', ',', '\n'] and nb_bracket == 0 and nb_quotes % 2 == 0:
                continue
            # If we are at the end of the file
            if c in [']', ''] and nb_bracket == 0 and nb_quotes % 2 == 0:
                # If EOF obtained or end of jsonarray send what's left of the data
                if example == "" or example == "]":
                    yield(data)
                    return
            if c == "\\":
                count_escape_char += 1
            else:
                count_escape_char = 0
            # Append character to the json example
            example += c

        # If at the end of the chunk, read new chunk
        if k == len(chunk) - 1:
            chunk = file_object.read(1000000)
        # Keep what's left of the chunk
        elif len(chunk) != 0:
            chunk = chunk[k:]
        # if k == 0 that means that we read the whole file
        else:
            break


def get_columns(list_data_paths, sep, logger, int_to_float, remove_null, is_json, flatten_list):
    """
        Get the columns created accordingly to a list of files containing json

        :param list_data_paths: list of files containing one json per line
        :param sep: separator to use when creating columns' names
        :param logger: logger (used to print)
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param is_json: if set to true, inputs are considered as valid json
        :param flatten_list: if set to true, will flatten the content of a list of objects

        :return: Exhaustive list of columns
    """

    columns_list = []

    j = 0
    chunk_size = 50000
    for data_file in list_data_paths:
        logger.info(data_file)
        json_list = []
        # If we deal with json (or json array) file
        if is_json:
            f = open(data_file)
            # Read json file by chunk
            for x in read_jsons_chunks(f, chunk_size=chunk_size):
                if j != 0 and (j % chunk_size == 0):
                    columns_list = update_columns_list(columns_list, json_list, sep, int_to_float, remove_null, flatten_list)
                    logger.info('Iteration ' + str(j) + ': Updating columns ===> ' + str(len(columns_list)) + ' columns found')
                    json_list = []
                try:
                    json_list.extend(x)
                    # Maximum of chunk_size elements were added
                    j += chunk_size
                except Exception:
                    logger.info("Json in line " + str(j) + " (in file: " + data_file + ") does not seem well formed. Example was skipped")
                    continue
        # If we deal with ljson
        else:
            with open(data_file) as f:
                for i, line in enumerate(f):
                    j += 1
                    if (j % 50000 == 0):
                        columns_list = update_columns_list(columns_list, json_list, sep, int_to_float, remove_null, flatten_list)
                        logger.info('Iteration ' + str(j) + ': Updating columns ===> ' + str(len(columns_list)) + ' columns found')
                        json_list = []
                    try:
                        json_list.append(json.loads(line))
                    except Exception:
                        logger.info("Json in line " + str(i) + " (in file: " + data_file + ") does not seem well formed. Example was skipped")
                        continue
        # A quicker solution would be to join directly to create a valid json
        if (len(json_list) > 0):
            columns_list = update_columns_list(columns_list, json_list, sep, int_to_float, remove_null, flatten_list)
            logger.info('Iteration ' + str(j) + ': Updating columns ===> ' + str(len(columns_list)) + ' columns found')

    # Concatenate the dataframes created
    logger.info('Full column\'s list obtained: ' + str(len(columns_list)) + ' fields found')
    return columns_list


def get_dataframe(list_data_paths, columns=None, path_csv=None, logger=None, sep='.', int_to_float=False, remove_null=False, is_json=False, flatten_list=False):
    """
        Get dataframe from files containing one json per line

        :param list_data_paths: list of files containing one json per line
        :param columns_list: list of columns to update
        :param path_csv: path to csv output if streaming
        :param logger: logger (used to print)
        :param sep: separator to use when creating columns' names
        :param int_to_float: if set to true int will be casted to float
        :param remove_null: if set to true, will remove_null from json arrays
        :param is_json: if set to true, inputs are considered as valid json
        :param flatten_list: if set to true, will flatten the content of a list of objects

        :return: dataframe or nothing if the dataframe is generated while streaming the files
    """

    json_list = []
    j = 0
    chunk_size = 50000
    for data_file in list_data_paths:
        logger.info(data_file)
        json_list = []
        # If we deal with json (or json array) file
        if is_json:
            f = open(data_file)
            # Read json file by chunk
            for x in read_jsons_chunks(f, chunk_size=chunk_size):
                if j != 0 and (j % chunk_size == 0):
                    logger.info('Iteration ' + str(j) + ': Creating sub dataframe')
                    if columns:
                        update_csv(path_csv, json_list, columns, sep, int_to_float, remove_null)
                        json_list = []
                try:
                    json_list.extend(x)
                    # Maximum of chunk_size elements were added
                    j += chunk_size  # -1 because we add 1 at the beginning of the loop
                except Exception:
                    logger.info("Json in line " + str(j) + " (in file: " + data_file + ") does not seem well formed. Example was skipped")
                    continue
        # If we deal with ljson
        else:
            with open(data_file) as f:
                for i, line in enumerate(f):
                    j += 1
                    if (j % 50000 == 0):
                        logger.info('Iteration ' + str(j) + ': Creating sub dataframe')
                        if columns:
                            update_csv(path_csv, json_list, columns, sep, int_to_float, remove_null)
                            json_list.clear()

                    if (j % 100000 == 0):
                        logger.info(str(i) + ' documents processed')
                    try:
                        json_list.append(json.loads(line))
                    except Exception:
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
        list_of_dfs = update_df_list([], json_list, sep, int_to_float, remove_null, flatten_list)
        logger.info('Concatenate ' + str(len(list_of_dfs)) + ' DataFrames')
        df = pd.concat(list_of_dfs)

        # Sort columns in alphabetical order
        columns_list = list(df.columns.values)
        columns_list.sort()

        return df[columns_list]
    else:
        return


def main(logger):
    """
        Main function of the program
    """

    # Load arguments
    opt = get_args()

    assert os.path.exists(opt.path_data_jsonperline)
    try:
        os.makedirs(os.path.dirname(opt.path_output))
    except Exception:
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
        columns_list = get_columns(data, opt.sep, logger, opt.int_to_float, opt.remove_null, opt.is_json)
        # Sort columns in alphabetical order
        columns_list.sort()
        df = pd.DataFrame(columns=columns_list)
        logger.info(columns_list)

        # Dump empty dataframes with columns
        df.to_csv(opt.path_output, encoding="utf-8", index=None, quoting=1)

    # Get dataframe
    df = get_dataframe(data, columns=columns_list, path_csv=opt.path_output, logger=logger, sep=opt.sep, int_to_float=opt.int_to_float, remove_null=opt.remove_null, is_json=opt.is_json, flatten_list=opt.flatten_list)

    if not opt.streaming:
        logger.info("saving data to " + opt.path_output)
        df.to_csv(opt.path_output, encoding="utf-8", index=None, quoting=1)

    logger.info('Csv successfully created and dumped')
    return 0


if __name__ == "__main__":
    try:
        logger = setup_custom_logger('json_to_csv_logger')
        sys.exit(main())
    except Exception as e:
        logger.info("Uncaught error waiting for scripts to finish")
        logger.info(e)
        raise
