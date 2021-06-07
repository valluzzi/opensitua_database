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
from opensitua_core import system_mail


class UsersDB(SqliteDB):
    """
    UsersDB - a class with common base methods
    """
    def __init__(self, dsn=":memory:", modules="", fileconf="", filekey="", verbose=False):
        """
        Constructor
        """
        SqliteDB.__init__(self, dsn)
        self.create_function("md5", 1, sqlite_md5 )
        self.__create_structure__(verbose)
        self.fileconf = fileconf
        self.filekey  = filekey

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

    def getAdmins(self, verbose=False):
        administrators = self.execute("""SELECT [mail] FROM [users] WHERE [role] ='admin';""", None, outputmode="array", verbose=verbose)
        administrators = [ item for (item,) in administrators]
        return administrators

    def sendMail(self, To, Object, text, env=None, verbose=False):
        system_mail(To, text, Object, "", self.fileconf, self.filekey, env)

    def sendMailToAdmin(self, Object, text , env=None, verbose=False):
        administrators = self.getAdmins()
        system_mail(administrators, text, Object, "", self.fileconf, self.filekey, env)

    def sendMailAndFeedBackAdmin(self, To, ObjectForUser, ObjectForAdmin="", text="", env=None, verbose=False):
        ObjectForAdmin = ObjectForAdmin if ObjectForAdmin else ObjectForUser
        system_mail(To, text, ObjectForUser, "", self.fileconf, self.filekey, env)
        system_mail(self.getAdmins(), text, ObjectForAdmin, To, self.fileconf, self.filekey, env)

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
            "token":sqlite_md5(f"{mail}{password}"),
            "enabled":1 if enabled else 0,
            "url":url
        }
        sql = "INSERT OR IGNORE INTO [users]([mail],[name],[token],[role],[enabled]) VALUES(?,?,?,?,?);"
        # def executeMany(self, sql, env={}, values=[], commit=True, verbose=False):
        self.executeMany(sql, env, [( env["mail"], env["name"], env["token"], env["role"], env["enabled"] )])

        __token__ = self.execute("SELECT [token] FROM [users] WHERE hex([name])=hex('{name}') AND hex([mail])=hex('{mail}');", env, outputmode="scalar", verbose=verbose)
        env["__token__"] = __token__

        if sendmail:
            text = ""  # some headers
            text+= "%s"%(extra)
            self.sendMailToAdmin( """Check user request of {name}""" , text, env )

        return __token__

    def existsUser(self, name, mail=""):
        """
        name
        """
        sql ="""SELECT COUNT(*) FROM [users] WHERE hex([mail]) = hex('{mail}');"""
        count = self.execute(sql, {"name":name,"mail":mail}, outputmode="scalar")
        return count>0


    def enableUser(self, token, enabled=1, sendmail=False, url="localhost", subject="", verbose=False):
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
        UPDATE [users] SET [enabled]={enabled},[token]=md5([mail]||'{password}') WHERE hex([token])=hex('{token}');
        SELECT [mail],[name],[enabled],md5([mail]||'{password}') as [token] FROM [users]  WHERE [token]=md5([mail]||'{password}');
        """
        (mail,name,enabled,token) = self.execute(sql, env, outputmode='first-row', verbose=verbose)

        # A mail to the user
        if mail:
            user = {
                "name":name,
                "mail":mail,
                "enabled":enabled,
                "password":env["password"],
                "token":token,
                "url":url
            }
            #Login at <a href='http://{url}/webgis/private/{mail}'>http://localhost/webgis/</a></br>
            text = """</br>
                    Hello {name}!</br>
                    Login at <a href='{url}'>{url}</a></br>
                    Your password is:<b>{password}</b></br>
                    """

            #if sendmail and os.path.isfile(self.fileconf):
            #    subject = subject if subject else "Credentials for the Web-Tool."
            #    system_mail(mail, text, subject, self.fileconf, self.filekey, user, verbose=verbose)
            ObjectForUser = subject if subject else "Credentials for the Web-Tool."
            ObjectForAdmin = "New User {name} has been enabled!"
            self.sendMailAndFeedBackAdmin(mail, ObjectForUser,ObjectForAdmin, text, user, verbose=verbose)
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
            WHERE hex([mail]) = hex('{username}')
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
        SELECT md5([token]||strftime('%Y-%m-%d','now'))='{__token__}' 
        FROM [users] 
            WHERE hex([mail])=hex('{__username__}') 
            LIMIT 1;
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
              UPDATE [users] SET [token]=md5([mail]||'{password}') WHERE hex([mail])=hex('{mail}');
              SELECT [name] FROM [users] WHERE hex([mail]) = hex('{mail}');
        """
        env["name"] = self.execute(sql, env, outputmode="scalar")

        if sendmail and os.path.isfile(self.fileconf):

            Subject = """Password change for {name} in {service}"""

            text = """
            <b>{name}</b> ({mail})<br>
            Your password has been changed:<br>
            password:<b>{password}</b>
            """
            #system_mail(mail, text, Subject, self.fileconf, self.filekey, env)
            ObjectForUser = Subject
            ObjectForAdmin = "User {name} changed the password"
            self.sendMailAndFeedBackAdmin(mail, ObjectForUser, ObjectForAdmin, text, env)

        return password