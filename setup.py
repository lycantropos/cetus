from setuptools import setup, find_packages

packages = find_packages()
setup(name='beylerbey',
      version='0.1.0',
      packages=packages,
      install_requires=[
          'SQLAlchemy>=1.0.12',
          # async
          'asyncio_extras',  # for async context managers
          'aiohttp',  # for working with http
          'asyncpg',  # for working with Postgres
          'aiomysql',  # for working with MySQL
      ],
      setup_requires=['pytest-runner'],
      tests_require=['SQLAlchemy-Utils>=0.32.12',  # for database creation/cleaning
                     'psycopg2==2.6.2',
                     'PyMySQL==0.7.10',
                     'pytest==3.0.5',
                     'pytest-cov==2.4.0',
                     'hypothesis==3.6.1',
                     'pytz'  # working with datetime objects in hypothesis
                     ])
