from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.config.configurable import Configurable
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory
from prettytable import PrettyTable

cluster = None
session = None

def load_ipython_extension(ipython):

    global cluster, session

    cluster = Cluster()
    session = cluster.connect()
    session.row_factory = ordered_dict_factory

    ipython.register_magics(CQLMagic)


def unload_ipython_extension(ipython):
    pass


class CQLResult(object):
    rows = None

    def __init__(self, result):
        self.rows = result

    def get_table(self):
        if not self.rows:
            return "<p>No results</p>"

        columns = self.rows[0].keys()
        table = PrettyTable(columns)
        for row in self.rows:
            table.add_row(row.values())

        return table

    def _repr_html_(self):
        table = self.get_table()
        return table.get_html_string()

    def __repr__(self):
        table = self.get_table()
        return table.get_string()



@magics_class
class CQLMagic(Magics, Configurable):
    @needs_local_scope
    @line_magic('cql')
    @cell_magic('cql')
    def execute(self, line, cell="", local_ns=None):
        global session

        if cell:
            line = cell
        result = session.execute(line)
        return CQLResult(result)

    @line_magic("keyspace")
    def set_keyspace(self, line, cell="", local_ns=None):
        print "Using keyspace %s" % line
        session.set_keyspace(line)


    @line_magic("tables")
    def get_tables(self, line, cell="", local_ns=None):
        return cluster.metadata.keyspaces[session.keyspace].tables.keys()
