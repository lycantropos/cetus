from setuptools import setup, find_packages

packages = find_packages()
setup(name='cetus',
      version='0.5.1',
      packages=packages,
      description='asynchronous working with PostgreSQL/MySQL '
                  'based on asyncpg/aiomysql',
      author='Azat Ibrakov',
      author_email='azatibrakov@gmail.com',
      url='https://github.com/lycantropos/cetus',
      download_url='https://github.com/lycantropos/cetus/archive/master.tar.gz',
      keywords=['async'],
      install_requires=[
          'sqlalchemy>=1.0.12',
          # async
          'asyncio_extras',  # async context managers
          'asyncpg',  # working with Postgres
          'aiomysql',  # working with MySQL
      ],
      setup_requires=['pytest-runner'],
      tests_require=['sqlalchemy-utils>=0.32.12',  # database creation/cleaning
                     'psycopg2==2.6.2',  # working with Postgres
                     'PyMySQL==0.7.10',  # working with MySQL
                     'pydevd',  # debugging
                     'pytest==3.0.5',
                     'pytest-asyncio',
                     'pytest-cov>=2.4.0',
                     'hypothesis>=3.6.1',
                     'pytz'  # working with datetime objects in hypothesis
                     ])
