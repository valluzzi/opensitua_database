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
import psycopg2
from .abstractdb import *
from opensitua_core import *

class PostgresDB(AbstractDB):
    """
    PostgresDB - a class with common base methods
    """
    def __init__(self, dsn="", env={}, verbose=False):
        """
        Constructor
        """
        self.dsn = dsn.format(**env)
        self.conn = None
        self.__connect__()

    def __connect__(self):
        """
        __connect__
        """
        try:
            self.conn = psycopg2.connect(self.dsn)

        except Exception as err:
            print(err)
            self.close()

