import sys
from java.lang import Class
from java.sql  import DriverManager, SQLException
from java.util import Properties
from org.apache.log4j import Level
from org.jetel.enums import EnabledEnum
from org.jetel.graph import TransformationGraph
from org.jetel.graph import Phase
from org.jetel.graph import Edge
from org.jetel.graph import JobType
from org.jetel.graph.runtime import GraphRuntimeContext
from org.jetel.graph.runtime import IThreadManager
from org.jetel.graph.runtime import SimpleThreadManager
from org.jetel.graph.runtime import WatchDog
from org.jetel.graph.runtime import WatchDogFuture
from org.jetel.component import DataReader
from org.jetel.component import DataWriter
from org.jetel.util.property import PropertyRefResolver

print "Here we go"

def remove_zero_files(in0, out0, out1):
    if (in0.size > 0):
        out0.name = in0.name
        print "non-zero size"
        return 0
    else:
        out1.name = in0.name
        print "zero size"
        return 1


class md_file:
    name = ''
    size = 0


def reformat(in_edge, func):
    out0 = md_file()
    out1 = md_file()

    func(in_edge, out0, out1)


in_edge = md_file()
in_edge.size = 1

reformat(in_edge, remove_zero_files)

id = "mygraph"

graph = TransformationGraph(id)
phase1 = Phase(100)
graph.addPhase(phase1)

dr = DataReader("dr", "test_file.csv", "UTF-8")
dr.setEnabled(EnabledEnum.ENABLED)
phase1.addNode(dr)

ed = Edge("e01", None)

dr.addOutputPort(0, ed)


# data writer
dw = DataWriter("dw", "processed_file.csv", "UTF-8", False)
dw.setEnabled(EnabledEnum.ENABLED)
phase1.addNode(dw)
dw.addInputPort(0, ed)

# an edge can be added to graph only if it has input and output nodes
# already assigned
phase1.addEdge(ed)


# run graph
additionalProperties = Properties();

runtimeContext = GraphRuntimeContext()
runtimeContext.setVerboseMode(False)
runtimeContext.addAdditionalProperties(additionalProperties)
#        if (trackingInterval > 0) 
runtimeContext.setTrackingInterval(20)
#runtimeContext.setSkipCheckConfig(skipCheckConfig);
runtimeContext.setUseJMX(False)
runtimeContext.setTokenTracking(False)
runtimeContext.setEdgeDebugging(False)
#runtimeContext.setDebugDirectory(debugDirectory);
runtimeContext.setContextURL(None)
runtimeContext.setLogLevel(Level.DEBUG)
runtimeContext.setJobType(JobType.ETL_GRAPH)
runtimeContext.setValidateRequiredParameters(False);
#        try {
#			runtimeContext.setJobUrl(FileUtils.getFileURL(contextURL, graphFileName).toString());
runtimeContext.setJobUrl("/home/daniel/src/jython-test/functest");
#		} catch (MalformedURLException e1) {
#			ExceptionUtils.logException(logger, "Given graph path cannot form a valid URL", e1);
#			ExceptionUtils.logHighlightedException(logger, "Given graph path cannot form a valid URL", e1);
#			System.exit(-1);
#		}
#       runtimeContext.setLocale(locale);
#        runtimeContext.setTimeZone(timeZone);

graph.checkConfig(None);

threadManager = SimpleThreadManager()
watchDog = WatchDog(graph, runtimeContext)

#graph.getRawComponentEnabledAttribute().get(dr)

#watchDog.call()

#threadManager.initWatchDog(watchDog);
#watchDogFuture = threadManager.executeWatchDog(watchDog);

