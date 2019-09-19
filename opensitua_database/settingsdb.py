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
# Name:        settingsdb.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     18/09/2019
# ------------------------------------------------------------------------------
import os,sys,re
from .sqlitedb import *
from .sqlite_utils import *


class SettingsDB(SqliteDB):
    """
    SettingsDB - a class with common base methods
    """
    def __init__(self, dsn=":memory:", modules="", verbose=False):
        """
        Constructor
        """
        SqliteDB.__init__(self, dsn)
        #self.create_function("md5", 1, sqlite_md5 )
        self.__create_structure__(verbose)

    def __create_structure__(self, verbose=False):
        """
        __create_structure__
        """
        sql = """
        CREATE TABLE IF NOT EXISTS [settings](
            key    TEXT,
            value  TEXT DEFAULT NULL, 
            [type] TEXT DEFAULT 'string',
            [groupname] TEXT DEFAULT 'General',
            PRIMARY KEY([key],[groupname])
        );"""
        self.execute(sql)

    def set(self, key, value, type='string', groupname='General'):
        """
        set - add or update a key,value tuple
        """
        sql ="""INSERT OR REPLACE INTO [settings]([key],[value],[type],[groupname]) VALUES(?,?,?,?);"""
        self.executeMany(sql,{},[(key,value,type,groupname)])

    def get(self, key, groupname='General'):
        """
        get - get a key, value
        """
        (value,type) = self.execute("""SELECT [value],[type] FROM [settings] WHERE [key] LIKE '{key}' AND [groupname]='{groupname}' LIMIT 1;""",{"key":key,"groupname":groupname},outputmode="first-row")
        return value