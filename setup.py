from setuptools import find_packages, setup, Command
from pathlib import Path
import sys
import os

# Project metadata
NAME = 'tickle'
DESCRIPTION = 'A command line workflow automation tool which performs task graph scheduling and concurrent task evaluation.'
URL = 'https://github.com/soren-n/tickle'
EMAIL = 'sorennorbaek@gmail.com'
AUTHOR = 'Soren Norbaek'
REQUIRES_PYTHON = '>=3.9.0'
REQUIRED = ['pyyaml', 'watchdog']

# Define long description
cwd = Path(__file__).parent
readme_path = Path(cwd, 'README.md')
with readme_path.open('r') as readme_file:
    LONG_DESCRIPTION = '\n%s' % readme_file.read()

# Define version
init_path = Path(cwd, './tickle/__init__.py')
with init_path.open('r') as init_file:
    VERSION = init_file.readline().split(' = ')[1][1:-1]

# Upload command
class UploadCommand(Command):
    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s): print('\033[1m{0}\033[0m'.format(s))
    def initialize_options(self): pass
    def finalize_options(self): pass

    def run(self):
        self.status('Removing previous builds ...')
        try: os.removedirs(Path(cwd, 'dist'))
        except: pass

        self.status('Building Source and Wheel (universal) distribution ...')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPI via Twine ...')
        os.system('twine upload dist/*')

        self.status('Pushing git tags ...')
        os.system('git tag v{0}'.format(VERSION))
        os.system('git push --tags')

        sys.exit()

setup(
    name = NAME,
    license = 'MIT',
    version = VERSION,
    description = DESCRIPTION,
    long_description = LONG_DESCRIPTION,
    long_description_content_type = 'text/markdown',
    url = URL,
    author = AUTHOR,
    author_email = EMAIL,
    requires_python = REQUIRES_PYTHON,
    install_requires = REQUIRED,
    packages = find_packages(exclude = ["tests", "examples"]),
    include_package_data = True,
    entry_points = {
        'console_scripts': ['tickle=tickle.main:cli'],
    },
    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Console'
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Microsoft',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9'
    ],
    cmdclass = {
        'upload': UploadCommand,
    }
)