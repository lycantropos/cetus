from setuptools import setup, find_packages

packages = find_packages()
setup(name='cetus',
      version='0.3.0',
      packages=packages,
      description='asynchronous working with PostgreSQL/MySQL '
                  'based on asyncpg/aiomysql',
      author='Azat Ibrakov',
      author_email='azatibrakov@gmail.com',
      url='https://github.com/lycantropos/cetus',
      download_url='https://github.com/lycantropos/cetus/archive/master.tar.gz',
      keywords=['async', 'MySQL', 'PostgreSQL'],
      install_requires=[
          'SQLAlchemy>=1.0.12',
          # async
          'asyncio_extras',  # for async context managers
          'asyncpg',  # for working with Postgres
          'aiomysql',  # for working with MySQL
      ],
      setup_requires=['pytest-runner'],
      tests_require=['SQLAlchemy-Utils>=0.32.12',  # for database creation/cleaning
                     'psycopg2==2.6.2',  # for working with Postgres
                     'PyMySQL==0.7.10',  # for working with MySQL
                     'pydevd',  # debugging
                     'pytest==3.0.5',
                     'pytest-asyncio',
                     'pytest-cov>=2.4.0',
                     'hypothesis>=3.6.1',
                     'pytz'  # working with datetime objects in hypothesis
                     ])
