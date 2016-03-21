#!/usr/bin/env python3

# This is a small script allowing to convert all files of FILE_TYPES in
# a directory tree SOURCE into a different format TARGET_TYPE saved to
# directory TARGET. All superfluous files in TARGET will be deleted.
#
# Currently no command line parsing is implemented, configure this
# script in the next section.

## Settings

SOURCE="/some/source/directory/"
TARGET="/some/target/directory/"

FILE_TYPES = [ "flac", "mp3", "opus", "ogg" ]
TARGET_TYPE= "opus"

# Regular expressions
IGNORE_PATHS = [ '\\.git/.*' ]

# Specify here the command and arguments for the conversion
def conversion_command(source, target):
    return ["nice", "ffmpeg", "-i", source, "-c:a", "libopus", "-b:a", "128k", "-vbr", "on", "-compression_level", "10", "-y", target]



## Start of programm

import os
import sys
import re
import itertools
import subprocess
import multiprocessing


def preprocess_settings():
    global FILE_TYPES
    global TARGET_TYPE
    global SOURCE
    global TARGET
    global IGNORE_PATHS

    tmp = FILE_TYPES
    FILE_TYPES = []
    for t in tmp:
        if not t.startswith("."):
            FILE_TYPES.append("." + t)

    if not TARGET_TYPE.startswith("."):
        TARGET_TYPE = "." + TARGET_TYPE

    SOURCE = os.path.normpath(SOURCE)
    TARGET = os.path.normpath(TARGET)

    tmp = IGNORE_PATHS
    IGNORE_PATHS = []
    for t in tmp:
        IGNORE_PATHS.append(os.path.join(SOURCE, "") + t)


# returns iterator over all files in given directory, filteres by ignore_paths and file_paths.
# Each file is given as (directory, filename)
def get_files(dir, ignore_paths, file_types):
    ignore_paths_compiled = list(map(re.compile, ignore_paths))

    for root, dirs, files in os.walk(dir):
        if any(map(lambda r: r.match(os.path.join(root, "")) is not None, ignore_paths_compiled)):
             continue

        for file in files:
            if any(map(lambda t: file.endswith(t), file_types)):
                yield os.path.join(root, file)


# returns iterator over ALL files in given directory
def get_files_all(dir):
    for root, dirs, files in os.walk(dir):
        for file in files:
            yield os.path.join(root, file)


# creates all directories for file
def create_dir(file):
    dir = os.path.dirname(file)
    os.makedirs(dir, exist_ok=True)


# Returns iterator whoch converts each element in source iterator in tuple with (source, target)
def source_paths_to_source_target_paths(xs, source_base, target_base, file_types, target_type):
    for x in xs:
        start = len(source_base) + 1 # +1 for directory seperator
        end = 0

        for t in file_types:
            if x.endswith(t):
                end = len(x) - len(t)
                break

        yield (x, os.path.join(target_base, x[start:end] + target_type))


# Gets and returns tuples of source_paths_to_source_target_paths. A tuple is returned of the source does exist.
# Useful for filtering symbolic links without existing target which are returned by os.walk()
def filter_existing_source(argument_tuples):
    for t in argument_tuples:
        if os.path.isfile(t[0]):
            yield t


# Gets and returns tuples of source_paths_to_source_target_paths. A tuple is returned of the target does not exist
def filter_existing_targets(argument_tuples):
    for t in argument_tuples:
        if not os.path.isfile(t[1]):
            yield t


def convert_file(source, target):
    try:
        subprocess.check_output(conversion_command(source, target), stderr=subprocess.PIPE, universal_newlines=True)
        return None
    except subprocess.CalledProcessError as ex:
        return ex.stderr


def __convert_files_inner(tuple):
    try:
        create_dir(tuple[1])
        result = convert_file(tuple[0], tuple[1])
        print(".", end="")
        sys.stdout.flush()
        return result
    except Exception as ex:
        return ex

# Converts the given tuples of source_paths_to_source_target_paths. Returns 0 if no error occurred
def convert_files(argument_tuples):

    with multiprocessing.Pool() as pool:
        errors = pool.map(__convert_files_inner, argument_tuples)

        errors = list(filter(lambda x: x is not None, errors))
        if len(errors) > 0:
            print("Errors occurred:", file=sys.stderr)
            for error in errors:
                print(error, file=sys.stderr)
            return 1

        print()
        return 0


# Deletes superfluous existing files in target dir. These are files, which are not a target of a source file.
def delete_superfluous_files(argument_tuples, existing_files):
    try:
        wanted_files = set()
        superfluous_files = []

        for t in argument_tuples:
            wanted_files.add(os.path.normpath(t[1]))

        for f in existing_files:
            normalized_f = os.path.normpath(f)

            if normalized_f not in wanted_files:
                superfluous_files.append(f)

        for f in superfluous_files:
            print("Deleting", f)
            os.remove(f)

    except Exception as e:
            print("Errors occurred:", e, file=sys.stderr)
            return 2

    for f in superfluous_files:
        try:
            os.removedirs(os.path.dirname(f))
        except Exception as e:
            pass

    return 0


preprocess_settings()

files = get_files(SOURCE, IGNORE_PATHS, FILE_TYPES)
source_target_tuples = source_paths_to_source_target_paths(files, SOURCE, TARGET, FILE_TYPES, TARGET_TYPE)
source_target_tuples, source_target_tuples_ = itertools.tee(source_target_tuples)
filtered_tuples = filter_existing_targets(filter_existing_source(source_target_tuples_))

errorcode = convert_files(filtered_tuples)

if errorcode == 0:
    existing_target_files = get_files_all(TARGET)
    errorcode = delete_superfluous_files(source_target_tuples, existing_target_files)

sys.exit(errorcode)