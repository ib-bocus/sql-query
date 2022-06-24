import re
import sys
import os
import mysql.connector

def tryint(s):
    try:
        return int(s)
    except:
        return s

def alphanum_key(s):

    return [ tryint(c) for c in re.split('([0-9]+)', s) ]

def sort_nicely(l):
    l.sort(key=alphanum_key)
    return l

# remove strings from a list that don't begin with a number
def remove_non_numbers(l):
    for x in l:
        if not x[0].isdigit():
            l.remove(x)
    return l

def remove_non_sql_files(l):
    for x in l:
        l[:] = [x for x in l if x.endswith('.sql')]
    return l

def read_script(filename, directory):
    filename = os.path.join(directory, filename)
    try:
        with open(filename, 'r') as f:
            sql = f.read()
            return sql
    except FileNotFoundError:
        raise Exception('File not found')
    except Exception as e:
        raise Exception(e)

def current_version(username, host, database, password):
    current_version = execute_sql('SELECT version from versionTable;', username, host, database, password)
    print(f"Current version: {current_version}")
    return current_version["version"]

def define_parameters():
    if len(sys.argv) != 6:
        raise Exception('Invalid number of parameters')
    directory = sys.argv[1]
    username = sys.argv[2]
    host = sys.argv[3]
    database = sys.argv[4]
    password = sys.argv[5]
    parameters = {'directory': directory, 'username': username, 'host': host, 'database': database, 'password': password}
    return parameters

def execute_sql(sql, username, host, database, password):
    # connect to database
    cnx = mysql.connector.connect(user=username, password=password, host=host, database=database, consume_results=True)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    try:
        cursor.execute(sql)
        cnx.commit()
        return cursor.fetchone()
    except:
        cnx.rollback()
        raise Exception('Error in executing sql')
    finally:
        cursor.close()
        cnx.close()
    return True

def order_scripts(directory):
    files = os.listdir(directory)
    files = [f for f in files if os.path.isfile(os.path.join(directory, f))]
    files = remove_non_numbers(files)
    files = remove_non_sql_files(files)
    files = sort_nicely(files)
    return files

def ignore_older_versions(files, current_version):
    files = [f for f in files if alphanum_key(f)[1] > current_version]
    return files

def update_version(username, host, database, password, version):
    execute_sql(f'UPDATE versionTable SET version = {version}', username, host, database, password)

# order scripts, then execute them
def execute_scripts(directory, username, host, database, password):
    files = order_scripts(directory)
    files = ignore_older_versions(files, current_version(username, host, database, password))

    print(f"Executing scripts: {files}")
    for file in files:
        sql = read_script(file, directory)
        execute_sql(sql, username, host, database, password)
        latest_version = alphanum_key(file)[1]
        print(f"latest version: {latest_version}")
        update_version(username, host, database, password, latest_version)

def main():
    parameters = define_parameters()
    execute_scripts(parameters['directory'], parameters['username'], parameters['host'], parameters['database'], parameters['password'])



main()
