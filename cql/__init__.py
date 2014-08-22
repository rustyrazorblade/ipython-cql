from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.config.configurable import Configurable
from cassandra.cluster import Cluster

cluster = None
session = None

def load_ipython_extension(ipython):

    global cluster, session

    cluster = Cluster()
    session = cluster.connect()

    ipython.register_magics(CQLMagic)


def unload_ipython_extension(ipython):
    pass



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
        return result

    @line_magic("keyspace")
    def set_keyspace(self, line, cell="", local_ns=None):
        print "Using keyspace %s" % line
        session.set_keyspace(line)


    @line_magic("tables")
    def get_tables(self, line, cell="", local_ns=None):
        return cluster.metadata.keyspaces[session.keyspace].tables.keys()
