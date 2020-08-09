#-------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2020 Valerio for Gecosistema S.r.l.
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
# Name:        opensitua_database
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:    23/07/2020
#-------------------------------------------------------------------------------
import os,sys,re
import json
from .postgresdb import *

def sformat(text, args):
    """
    sformat
    """
    for key in args:
        text = text.replace("{%s}" % key, "%s" % (args[key]))

    text = re.sub(r'\{(.*?)\}',r'',text)
    return text


def JSONResponse(obj, start_response=None):
    """
    JSONResponse
    """
    if isinstance(obj, str):
        text = obj
    elif isinstance(obj, (dict, list)):
        text = json.dumps(obj)
    else:
        text = obj

    response_headers = [('Content-type', 'application/json'), ('Content-Length', str(len(text)))]
    if start_response:
        start_response("200 OK", response_headers)
    return [text.encode('utf-8')]

def SqlScriptResponse( params, start_response ):
    """
    SqlScriptResponse - exec an sctipt
    """
    res = []
    db = None
    sql = ""
    verbose = False

    #params = Params(environ).toDictionary()
    DOCUMENT_WWW = params["DOCUMENT_WWW"]
    filename = params["filename"] if "filename" in params else ""
    filename = DOCUMENT_WWW + "/" + filename
    filename = re.sub(r'\.\.', '', filename)

    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            line = f.readline()
            while line:
                # search for debug mode
                pattern = r'^\#\s*(DEBUG\s*=\s*TRUE).*'
                m = re.search(pattern, line, re.I | re.S)
                verbose = True if m else False

                pattern = r'^\s*SELECT\s+[\',\"](?P<dsn>.*host\s*=.*)[\',\"]\s*;'
                m = re.search(pattern, line, re.I | re.S)
                dsn = m.group('dsn') if m else ""
                if dsn:
                    if db:
                        db.close()
                    db = PostgresDB(dsn)
                    sql = ""
                else:
                    # catenate text
                    sql += line

                line = f.readline()

            if db and sql:
                # print(sformat(sql,params))
                res = db.execute(sql, params, outputmode="dict", verbose=verbose)

            if db:
                db.close()

            return JSONResponse(res, start_response)

    return JSONResponse({"exception": "filename <%s> doesnot exits!" % (filename)}, start_response)




