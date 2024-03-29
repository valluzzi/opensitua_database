#-------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2019 Valerio for Gecosistema S.r.l.
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
# Created:    11/09/2019
#-------------------------------------------------------------------------------
import hashlib

def sqlite_md5(text):
    """
    md5 - Returns the md5 of the text
    """
    if (text!=None):
        hash = hashlib.md5()
        hash.update(text.encode("utf-8"))
        return hash.hexdigest()
    return None


