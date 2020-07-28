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
from .abstractdb import *
from opensitua_core import *

class SqliteDB(AbstractDB):
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

