# ------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2021 Luzzi Valerio
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
# Name:        jobsdb.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     12/03/2021
# ------------------------------------------------------------------------------
import os,sys,re
import subprocess, psutil
from .sqlitedb import *
from .sqlite_utils import *


class JobsDB(SqliteDB):
    """
    JobsDB - a class with common base methods
    """
    def __init__(self, dsn=":memory:", modules="", fileconf="", filekey="", verbose=False):
        """
        Constructor
        """
        SqliteDB.__init__(self, dsn)
        self.create_function("md5", 1, sqlite_md5 )
        self.__create_structure__(verbose)

    def __create_structure__(self, verbose=False):
        """
        __create_structure__
        """
        sql = """
        CREATE TABLE IF NOT EXISTS [jobs](
              [jid] TEXT, 
              [pid] INT, 
              [precond_jid] INTEGER,
              [type] TEXT, 
              [user] TEXT, 
              [description] TEXT, 
              [command] TEXT, 
              [status] TEXT DEFAULT ready, 
              [progress] INT DEFAULT 0, 
              [inserttime] INTEGER DEFAULT (STRFTIME ('%s', 'now', 'localtime')), 
              [starttime] DATETIME, 
              [endtime] DATETIME, 
              PRIMARY KEY([jid], [user])) WITHOUT ROWID;"""
        self.execute(sql)


    def addJob(self, options, verbose=False):
        """
        addUser
        """

        env = {
            "jid":"0000",
            "pid":os.getpid(),
            "type":"generic",
            "user":"unknown",
            "description":"...",
            "command":"",
            "status":"ready",
            "progress":0.0,
            "precond_jid": 0,
        }
        env.update(options)
        sql= """INSERT OR IGNORE INTO [jobs](  [jid],  [user],  [pid],  [precond_jid], [type],   [description],  [command],  [status], [progress]) 
                                      VALUES('{jid}','{user}','{pid}','{precond_jid}','{type}','{description}','{command}','{status}','{progress}')"""
        self.execute(sql, env, verbose=verbose)

    def executeJob(self, jid, white_list=""):
        """
        executeJob
        """
        params ={"jid" : jid}

        sql = """UPDATE [jobs] SET status='running',progress=0 WHERE jid='{jid}';
                 SELECT [command] FROM [jobs] WHERE [jid]='{jid}' and status='running';"""
        command = self.execute(sql, params, outputmode="scalar")

        # -- Security filter to allow just commands in white_list ------------------------------------------------------
        if command:
            commandname = command.split(" ",1)[0]
            white_list = listify(white_list)
            if not commandname.lower() in white_list:
                command = ""
                params["pid"] = -1
                params["status"] = "error"
                params["progress"] = 100
                params["starttime"] = ""
                params["description"] = "Command not enabled!"
                # print("{description}".format(**params))
            if "&" in command:
                command = ""
                params["pid"] = -1
                params["status"] = "error"
                params["progress"] = 100
                params["starttime"] = ""
                params["description"] = "Command catenation not allowed!"
                # print("{description}".format(**params))
        # --------------------------------------------------------------------------------------------------------------
        if command:
            try:
                p = subprocess.Popen(command)
                print("Running <%s> with pid:%d" % (command, p.pid))
                print("-----------------------------------------")
                params["pid"] = p.pid
                params["starttime"] = strftime('%Y-%m-%d %H:%M:%S', None)
                params["status"] = "running"
                params["progress"] = 0
            except Exception as ex:
                params["pid"] = -1
                params["status"] = "error"
                params["progress"] = 100
                params["starttime"] = ""
        else:
            params["pid"] = -1
            params["status"] = "error"
            params["progress"] = 100
            params["starttime"] = ""


        sql = """UPDATE [jobs] SET status='{status}', pid='{pid}', progress={progress}, starttime='{starttime}' WHERE jid='{jid}';"""
        self.execute(sql, params)

    def ProcessQueue(self, parallelism = -1, max_load = 70, white_list = "", verbose = False):
        """
        ProcessQueue - process the job queue
        """
        parallelism = parallelism if parallelism else psutil.cpu_count()
        sql = """SELECT a.* FROM [jobs] a 
                LEFT JOIN [jobs] b ON a.[precond_jid] = b.[jid]
                WHERE a.[status] = 'queued' AND ( b.[status] = 'done' or b.[status] IS NULL)
                ORDER BY a.[inserttime] ASC;"""
        job_list = self.execute(sql, outputmode="dict", verbose=verbose)
        for job in job_list:
            n = self.execute("""SELECT COUNT(*) FROM [jobs] WHERE [status] NOT IN ('ready','queued','done','error');""",
                outputmode="scalar", verbose=verbose)
            cpu_load = psutil.cpu_percent(interval=1)
            if n < parallelism or cpu_load < max_load:
                self.executeJob(job["jid"], white_list)

    def ProcessQueueForever(self, parallelism = -1, max_load = 70, interval = 3, white_list = "", verbose=False):
        """
        ProcessQueueForever
        """
        while True:
            self.ProcessQueue(parallelism, max_load, white_list, verbose=verbose)
            sleep(interval)

