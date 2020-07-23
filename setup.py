import os,re
import setuptools



PACKAGE_NAME    = "opensitua_database"
AUTHOR          = "Valerio Luzzi"
EMAIL           = "valluzzi@gmail.com"
GITHUB          = "https://github.com/valluzzi/%s.git"%(PACKAGE_NAME)
DESCRIPTION     = "A core functions package"

VERSION         =  '0.0.71

setuptools.setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    url=GITHUB,
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    install_requires=['opensitua_core','psycopg2']
)
