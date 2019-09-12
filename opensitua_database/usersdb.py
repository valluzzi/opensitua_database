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
from opensitua_core import *


class UsersDB(SqliteDB):
    """
    UsersDB - a class with common base methods
    """
    def __init__(self, dsn=":memory:", modules="", fileconf="", verbose=False):
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

    def addUser(self, mail, name="", password="", role="user", enabled=False, sendmail=False, verbose=False):
        """
        addUser
        """
        password = sqlite_md5(password) if password else sqlite_md5("%s"%(randint(0,100000)))[:5]
        env = {
            "mail":mail,
            "name": name if name else mail,
            "password": password,
            "role":role,
            "enabled":1 if enabled else 0
        }
        sql= """
        INSERT OR IGNORE INTO [users]([mail],[name],[token],[role],[enabled]) VALUES('{mail}','{name}',md5('{mail}'||'{password}'),'{role}',{enabled});
        SELECT [token] FROM   [users] WHERE [name] ='{name}' AND [mail]='{mail}';
        """
        __token__ = self.execute(sql, env, outputmode="scalar", verbose=verbose)
        env["__token__"] = __token__

        #send a mail to Administrators
        administrators = self.execute("""SELECT GROUP_CONCAT([mail],',') FROM [users] WHERE [role] ='admin';""", env,
                               outputmode="scalar", verbose=verbose)

        if administrators and sendmail and isfile(self.fileconf):
            text = """</br>
                   {name} ask you to grant access to the Web Tool.</br>
                   If you want to enable {name} aka {mail} click on the following link:</br>
                   <a href='http://localhost/common/lib/py/users/enable.py?token={__token__}&enabled=1'>Enable {name}</a></br>
                   """
            system_mail(administrators, sformat(text, env), sformat("""User confirmation of {name}""", env), self.fileconf)

        return __token__
