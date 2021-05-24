from pathlib import Path
from setuptools import find_packages, setup

def _long_description():
    readme = Path(__file__).parent / 'README.md'
    with readme.open(encoding='utf-8') as f:
        return r.read()

NAME = 'asq-store-ooq-pipeline'
VERSION = '0.1.0'
PACKAGES = find_packages(where='src')
PACKAGE_DIR = {'': 'src'}
INSTALL_REQUIRES = []
EXTRAS_REQUIRE = {
    'dev': [
        'pytest'
    ]
}

params = {
    'name': NAME,
    'version': VERSION,
    'packages': PACKAGES,
    'package_dir': PACKAGE_DIR,
    'install_requires': INSTALL_REQUIRES,
    'extras_require': EXTRAS_REQUIRE
}

setup(**params)

