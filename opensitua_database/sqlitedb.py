# ------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2018 Luzzi Valerio
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
#
# Name:        abstractdb.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     31/07/2018
# ------------------------------------------------------------------------------
import os,sys,re
import time
import sqlite3 as sqlite
from opensitua_core import *

class SqliteDB:
    """
    SqliteDB - a class with common base methods
    """
    def __init__(self, dsn=":memory:", modules="", verbose=False):
        """
        Constructor
        """
        mkdirs(justpath(dsn))
        self.dsn = dsn
        self.conn = None
        self.__connect__()
        self.pragma("synchronous=OFF", verbose=verbose)
        self.pragma("journal_mode=WAL", verbose=verbose)
        self.pragma("foreign_keys=ON", verbose=verbose)
        self.pragma("cache_size=4096", verbose=verbose)
        self.load_extension(modules, verbose=verbose)
        self.conn.enable_load_extension(True)


    def close(self, verbose=False):
        """
        Close the db connection
        """
        if self.conn:
            if verbose:
                print("closing db...")
            self.conn.close()

    def commit(self):
        """
        force commit
        """
        if self.conn:
            self.conn.commit()

    def __del__(self):
        """
        destructor
        """
        self.close()

    def __get_cursor__(self):
        """
        __get_cursor__
        """
        return self.conn.cursor()

    def __connect__(self):
        """
        __connect__
        """
        try:
            if not self.dsn.startswith(":"):
                self.dsn = forceext(self.dsn, "sqlite") if justext(self.dsn) == "" else self.dsn
            self.conn = sqlite.connect(self.dsn)

        except sqlite.Error as err:
            print(err)
            self.close()

    def pragma(self, text, env={}, verbose=True):
        """
        pragma
        """
        try:
            text = sformat(text,env)
            if verbose:
                print("PRAGMA " + text)
            self.conn.execute("PRAGMA " + text)
        except sqlite.Error as err:
            print(err)

    def create_function(self, func, nargs, fname):
        """
        create_function
        """
        self.conn.create_function(func, nargs, fname)

    def create_aggregate(self, func, nargs, fname):
        """
        create_aggregate
        """
        self.conn.create_aggregate(func, nargs, fname)

    def load_extension(self, modules, verbose=False):
        """
        load_extension
        """
        try:
            modules = listify(modules)
            self.conn.enable_load_extension(True)
            if isLinux() or isMac():
                modules = [os.join(justpath(item), juststem(item)) for item in modules]

            for module in modules:
                try:
                    self.conn.execute("SELECT load_extension('%s');" % (module))
                    if verbose:
                        print("loading module %s...ok!" % (module))
                except OperationalError as ex:
                    print("I can't load  %s because:%s" % (module,ex))

            self.conn.enable_load_extension(False)
        except(Exception):
            print("Unable to load_extension...")

    def __prepare_query__(self, sql, env={}, verbose=False):
        """
        prepare the query
        remove comments and blank lines
        """
        comment1 = "--"
        comment2 = "#"
        comment3 = "//"
        sql = re.sub(r'(\r\n|\n)','\n',sql)
        lines = split(sql, "\n", "'\"")

        # follow statement remove comments after SQL line code.
        lines = [split(line, comment1, "'\"")[0] for line in lines]
        lines = [split(line, comment2, "'\"")[0] for line in lines]
        lines = [split(line, comment3, "'\"")[0] for line in lines]
        lines = [line.strip(" \t") for line in lines]

        # follow statement remove all lines of comments
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment1)]
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment2)]
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment3)]

        sql = " ".join(lines)
        #remove spaces between stetements
        sql = re.sub(r';\s+',';',sql)

        #env = self.__check_args__(env)

        return sql, env

    def execute(self, sql, env=None, outputmode="array", commit=True, verbose=False):
        """
        Make a query statement list
        Returns a cursor
        """
        rows = []
        cursor = self.__get_cursor__()
        env = env.copy() if env else {}
        env.update(os.environ)
        if cursor:
            sql, env = self.__prepare_query__(sql, env, verbose)

            sql = sformat(sql, env)
            commands = split(sql, ";", "'\"")
            commands = [command.strip() + ";" for command in commands if len(command) > 0]

            for command in commands:
                t1 = time.time()
                command = sformat(command, env)

                try:

                    cursor.execute(command)

                    if commit==True and not command.upper().strip(' \r\n').startswith("SELECT"):
                         self.conn.commit()

                    env.update(os.environ)

                    t2 = time.time()

                    if verbose:
                        command = command.replace("\n", " ")
                        print("->%s:Done in (%.4f)s" % (command[:], (t2 - t1)))

                except Exception as ex:
                    command = command.replace("\n", " ")
                    print( "No!:SQL Exception:%s :(%s)"%(command,ex))

                    if outputmode == "response":
                        res = {"status": "fail", "success": False, "exception": ex, "sql": command}
                        return res

            if outputmode == "cursor":
                return cursor

            elif outputmode == "array":
                for row in cursor:
                    rows.append(row)

            elif outputmode == "scalar":
                row = cursor.fetchone()
                if row and len(row):
                    return row[0]
                return None

            elif outputmode == "first-row":
                row = cursor.fetchone()
                if row and len(row):
                    return row
                if cursor.description:
                    return tuple([None]*len(cursor.description))
                return None

            elif outputmode == "table":
                metadata = cursor.description
                if metadata:
                    rows.append(tuple([item[0] for item in metadata]))
                for row in cursor:
                    rows.append(row)

            elif outputmode in ( "object", "dict" ):
                if cursor.description:
                    columns = [item[0] for item in cursor.description]
                    for row in cursor:
                        line = {}
                        for j in range(len(row)):
                            line[columns[j]] = row[j]
                        rows.append(line)

            elif outputmode == "columns":
                n = len(cursor.description)
                rows = [[] for j in range(n)]
                for row in cursor:
                    for j in range(n):
                        rows[j].append(row[j])

            elif outputmode in ("response","row-response"):
                metadata = []
                res = {}
                if cursor.description:
                    metadata = cursor.description
                    columns = [item[0] for item in cursor.description]
                    for row in cursor:
                        line = {}
                        for j in range(len(row)):
                            line[columns[j]] = row[j]
                        rows.append(line)

                    res = {"status": "success", "success": True, "data": rows, "metadata": metadata, "exception": None}
                return res

            elif outputmode == "column-response":
                metadata = []
                res = {}
                if cursor.description:
                    metadata = cursor.description
                    columns = [item[0] for item in cursor.description]
                    selection={}
                    for row in cursor:
                        for j in range(len(row)):
                            if not columns[j] in selection:
                                selection[columns[j]] = []
                            selection[columns[j]].append(row[j])

                    res = {"status": "success", "success": True, "data": selection, "metadata": metadata, "exception": None}
                return res

        return rows


    def executeMany(self, sql, env={}, values=[], commit=True, verbose=False):
        """
        Make a query statetment
        """
        cursor = self.__get_cursor__()
        line = sformat(sql, env)
        try:
            t1 = time.time()
            cursor.executemany(line, values)
            if commit:
                self.conn.commit()
            t2 = time.time()
            if verbose:
                line = line.replace("\n", " ")
                print("->%s:Done in (%.2f)s" % (line[:], (t2 - t1)))

        except Exception as ex:
            line = line.replace("\n", " ")
            print( "No!:SQL Exception:%s :(%s)"%(line,ex))

    def insertMany(self, tablename, values=[], mode ="IGNORE", fieldnames = "",commit=True, verbose=False):
        """
        Make an insert statetment
        """
        cursor = self.__get_cursor__()
        fieldnames = wrap(listify(fieldnames), "[", "]")
        print(fieldnames)
        print("---")
        if len(values):
            n = len(fieldnames) if len(fieldnames) else len(values)

            env ={
                mode        :   mode,
                tablename   : tablename,
                fieldnames  : ",".join( fieldnames ),
                question_marks : ",".join( ["?"]*n )
            }
            sql = """INSERT OR {mode} INTO [{tablename}]({fieldnames}) VALUES ({question_marks});"""
            print(sformat(sql,env))
            self.executeMany(sql, env, values, commit, verbose)

    def list(self, verbose=False):
        """
        list tables
        """
        sql = """
        SELECT [tbl_name] FROM [sqlite_master] WHERE LOWER([type]) in ('table','view')
        UNION ALL
        SELECT [tbl_name] FROM [sqlite_temp_master] WHERE LOWER([type]) in ('table','view');
        """
        return self.execute(sql, {}, outputmode="array", verbose=verbose)