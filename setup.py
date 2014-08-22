from distutils.core import setup

version = open('cql/VERSION', 'r').readline().strip()

long_desc = """
IPython extension for Cassandra Query Language
"""


setup(
    name='ipython-cql',
    version=version,
    packages=['cql'],
    url='https://github.com/rustyrazorblade/ipython-cql',
    license='2 Clause BSD',
    author='jhaddad',
    author_email='jon@jonhaddad.com',
    description='IPython Extension for Cassandra integration',
    install_requires=["cassandra-driver"]
)
