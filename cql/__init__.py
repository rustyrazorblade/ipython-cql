from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.config.configurable import Configurable
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory, SimpleStatement
from prettytable import PrettyTable

try:
    import numpy as np
    import matplotlib.mlab as mlab
    import matplotlib.pyplot as plt
except:
    pass

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


class TablePrinter(object):
    def __init__(self, table):
        self.table = table

    def _repr_html_(self):
        return self.table.get_html_string()

    def __repr__(self):
        return self.table.get_string()


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

        if not result:
            return "No results."

        columns = result[0].keys()
        table = PrettyTable(columns)
        for row in result:
            table.add_row(row.values())

        return TablePrinter(table)

    @line_magic("keyspace")
    def set_keyspace(self, line, cell="", local_ns=None):
        print "Using keyspace %s" % line
        session.set_keyspace(line)

    @line_magic("tables")
    def get_tables(self, line, cell="", local_ns=None):
        table = PrettyTable(["name", "partition key", "clustering key", "compaction"])

        for name, tab in cluster.metadata.keyspaces[session.keyspace].tables.iteritems():
            pks = ",".join([x.name for x in tab.partition_key])
            clustering_key = ",".join([x.name for x in tab.clustering_key])

            compaction = tab.options['compaction_strategy_class'].replace("org.apache.cassandra.db.compaction.", "")

            table.add_row([name, pks, clustering_key, compaction])

        return TablePrinter(table)

        # return cluster.metadata.keyspaces[session.keyspace].tables.keys()


    @cell_magic("histogram")
    @line_magic("histogram")
    def get_histogram(self, line, cell="", local_ns=None):

        if cell:
            line = cell
        
        mu, sigma = 100, 15

        # the histogram of the data
        global session
        result = session.execute(line)

        key = result[0].keys()[0]
        x = [v[key] for v in result]

        n, bins, patches = plt.hist(x, 50, normed=1, facecolor='green', alpha=0.75)

        # add a 'best fit' line
        y = mlab.normpdf(bins, mu, sigma)
        l = plt.plot(bins, y, 'r--', linewidth=1)

        #plt.xlabel('Smarts')
        #plt.ylabel('Probability')
        plt.title(r'$\mathrm{quick histogram}\ \mu=100,\ \sigma=15$')
        plt.axis([0, 10, 0, .13])
        plt.grid(True)
        plt.show()

    @line_magic("desc")
    def describe(self, line, cell="", local_ns=None):
        table = cluster.metadata.keyspaces[session.keyspace].tables[line]
        print table.export_as_string()


    @line_magic("trace")
    def get_trace(self, line, cell="", local_ns=None):

        global session

        if cell:
            line = cell

        query = SimpleStatement(line)
        result = session.execute(query, trace=True)
        return Trace(query)


class Trace(object):
    def __init__(self, query):
        # accepts a simple statement
        self.trace = query.trace

    def get_table(self):
        table = PrettyTable(["activity", "timestamp", "source", "source_elapsed"])
        for event in self.trace.events:
            table.add_row([event.description, event.datetime, event.source, event.source_elapsed])

        return table

    def _repr_html_(self):
        table = self.get_table()
        return table.get_html_string()
