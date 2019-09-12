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
import hashlib
from .sqlitedb import *
from .sqlite_utils import *


class UsersDB(SqliteDB):
    """
    UsersDB - a class with common base methods
    """
    def __init__(self, dsn=":memory:", modules="", verbose=False):
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
        CREATE TABLE IF NOT EXISTS [users](
            mail TEXT PRIMARY KEY,
            name TEXT, 
            token TEXT(32), 
            enabled BOOL DEFAULT 0,
            role TEXT DEFAULT 'user'
        );"""
        self.execute(sql)