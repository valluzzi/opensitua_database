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
from random import randint
from .sqlitedb import *
from .sqlite_utils import *
from opensitua_core import system_mail,sformat


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
        self.fileconf = fileconf


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

    def sendMail(self, to, Object, text, env=None, verbose=False):
        if to and os.path.isfile(self.fileconf):
            env = env if env else {}
            system_mail(to, sformat(text,env), sformat(Object,env), self.fileconf, verbose=verbose)

    def sendMailToAdmin(self, Object, text ,  env=None, verbose=False):

        administrators = self.execute("""SELECT GROUP_CONCAT([mail],',') FROM [users] WHERE [role] ='admin';""", None,outputmode="scalar", verbose=verbose)
        if administrators and  os.path.isfile(self.fileconf):
            env = env if env else {}
            system_mail(administrators, sformat(text,env), sformat(Object,env), self.fileconf, verbose=verbose)

    def addUser(self, mail, name="", password="", role="user", enabled=False, sendmail=False, url="localhost", extra="", verbose=False):
        """
        addUser
        """
        password = sqlite_md5(password) if password else sqlite_md5("%s"%(randint(0,100000)))[:5]
        env = {
            "mail":mail,
            "name": name if name else mail,
            "password": password,
            "role":role,
            "enabled":1 if enabled else 0,
            "url":url
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
            text = ""  #some headers
            text+= "%s"%(extra)
            system_mail(administrators, sformat(text, env), sformat("""Check user request of {name}""", env), self.fileconf)

        return __token__

    def enableUser(self, token, enabled=1, sendmail=False, url="localhost", verbose=False):
        """
        enableUser
        """
        env ={
            "token":token,
            "enabled":1 if enabled else 0,
            "password":  sqlite_md5("%s"%randint(0,10000))[:5]
        }
        #Abilita l'utente e gli cambia il token

        sql = """
        UPDATE [users] SET [enabled]={enabled},[token]=md5([mail]||'{password}') WHERE [token]='{token}';
        SELECT [mail],[name],[enabled] FROM [users]  WHERE [token]=md5([mail]||'{password}');
        """
        (mail,name,enabled) = self.execute(sql, env, outputmode='first-row', verbose=verbose)

        # A mail to the user
        if mail:
            user = {
                "name":name,
                "mail":mail,
                "enabled":enabled,
                "password":env["password"],
                "url":url
            }
            #Login at <a href='http://{url}/webgis/private/{mail}'>http://localhost/webgis/</a></br>
            text = """</br>
                    Login at <a href='{url}'>{url}</a></br>
                    Your password is:<b>{password}</b></br>
                    """

            if sendmail and os.path.isfile(self.fileconf):
                system_mail(mail, sformat(text, user), "Credentials for the Web-Tool.", self.fileconf,verbose=verbose)
            return user

        return False

    def getToken(self,username, password):
        """
        getToken
        """
        env = {
            "username": username,
            "password": password
        }
        sql = """
        SELECT md5([token]||strftime('%Y-%m-%d','now')) FROM [users]
            WHERE ([name] LIKE '{username}' OR [mail] LIKE '{username}')
            AND [token] LIKE md5([mail]||'{password}')
            AND [enabled];
        """
        return self.execute(sql, env, outputmode="scalar", verbose=False)

    def isValid(self, username, token):
        """
        checkToken -  check token is valid
        """
        env = {
            "__username__": username,
            "__token__": token
        }
        sql = """
        SELECT md5([token]||strftime('%Y-%m-%d','now'))='{__token__}' FROM [users] WHERE [mail] LIKE '{__username__}' LIMIT 1;
        """
        return self.execute(sql, env, outputmode="scalar", verbose=False)

    def changePassword(self, mail, sendmail=False, service="localhost"):
        """
        changePassword
        """
        password = sqlite_md5("%s" % (randint(0, 100000)))[:5]
        env = {
            "mail": mail,
            "password": password,
            "service": service
        }
        sql = """
              UPDATE [users] SET [token]=md5([mail]||'{password}')  WHERE [mail] LIKE '{mail}';
        """
        self.execute(sql, env, outputmode="scalar")
        if sendmail and os.path.isfile(self.fileconf):

            Subject = """Password change for {service}"""

            text = """
            {service}<br>
            Your password was been changed:<br>
            password:<b>{password}</b>
            """
            system_mail(mail, sformat(text, env), sformat(Subject, env), self.fileconf)

        return password