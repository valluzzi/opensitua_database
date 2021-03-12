# ------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2020 Luzzi Valerio
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
import psycopg2
from opensitua_core import *

SQL_INJECTION_WORDS = (

    "CREATE ",
    "INSERT ",
    "UPDATE ",
    "REPLACE ",
    "SELECT "
    "DROP ",
    "DELETE ",

)


class AbstractDB:
    """
    AbstractDB - a class with common base methods
    """
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
        pass

    def __contains__(self, text, items, case_sensitive=True):
        """
        __contains__
        """
        for item in items:
            if case_sensitive and "%s"%item in text:
                return True
            elif not case_sensitive and ("%s"%item).upper() in text.upper():
                return True
        return False

    def __sql_injection__(self, text):
        """
        avoid sql injection
        """
        if isinstance(text,str) and self.__contains__(text, SQL_INJECTION_WORDS, False):
            print("Warning:SQL INJECTION DETECTED!:<%s>"%text)
            return ""
        #if text.strip('\t\n\r ').startswith("'")

        return text

    def __check_args__(self, env):
        """
        check for sql injection
        """
        for key in env:
            env[key] = self.__sql_injection__(env[key])
        return env

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

        #check for sql injection
        env = self.__check_args__(env)

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

                    if outputmode in ("response","row-response"):
                        res = {"status": "fail", "success": False, "exception": "%s"%ex, "sql": command}
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

                    res = {"status": "success", "success": True, "data": rows, "metadata": columns, "pos":0, "total_count":len(rows), "exception": None}
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
        if len(values):
            n = len(fieldnames) if len(fieldnames) else len(values)

            env ={
                "mode"        :   mode,
                "tablename"   : tablename,
                "fieldnames"  : ",".join( fieldnames ),
                "question_marks" : ",".join( ["?"]*n )
            }
            sql = """INSERT OR {mode} INTO [{tablename}]({fieldnames}) VALUES ({question_marks});"""
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


