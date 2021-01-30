import java.util.Properties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jetel.component.DataReader;
import org.jetel.component.DataWriter;
import org.jetel.graph.Edge;
import org.jetel.graph.JobType;
import org.jetel.graph.Phase;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.graph.runtime.WatchDogFuture;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.util.property.PropertyRefResolver;
import org.python.util.PythonInterpreter;
import org.python.core.*;

public class Test {

    public static void runGraph() {
        try {
            Logger.getLogger(Test.class); // make log4j to init itself
            String pluginsRootDirectory = null;
            String licenseLocations = null;
            String logHost = null;
            String graphFileName = null;
            String configFileName = null;
            EngineInitializer.initEngine(pluginsRootDirectory, configFileName, logHost);

            String id = "mygraph";
            TransformationGraph graph = new TransformationGraph(id);
            Phase phase1 = new Phase(100);
            graph.addPhase(phase1);

            DataReader dr = new DataReader("dr", "test_file.csv", "UTF-8");
            phase1.addNode(dr);

            dr.getPropertyRefResolver();

            DataWriter dw = new DataWriter("dw", "processed_file.csv", "UTF-8", false);
            phase1.addNode(dw);

            // metadata
            DataRecordMetadata md = new DataRecordMetadata("md_test", 'D');
            md.setRecordDelimiter("\n");
            md.setFieldDelimiter(";");
            DataFieldMetadata f1 = new DataFieldMetadata("Name", DataFieldType.STRING, 100);
            md.addField(f1);
            DataFieldMetadata f2 = new DataFieldMetadata("Surname", DataFieldType.STRING, 100);
            md.addField(f2);

            // edge
            Edge ed = new Edge("e01", md);
            
            dr.addOutputPort(0, ed);
            dw.addInputPort(0, ed);

            phase1.addEdge(ed);

            // run it
            
            Properties additionalProperties = new Properties();

            GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
            runtimeContext.setVerboseMode(false);
            runtimeContext.addAdditionalProperties(additionalProperties);
            runtimeContext.setTrackingInterval(20);
            //runtimeContext.setSkipCheckConfig(skipCheckConfig);
            runtimeContext.setUseJMX(false);
            runtimeContext.setTokenTracking(false);
            runtimeContext.setEdgeDebugging(false);
            //runtimeContext.setDebugDirectory(debugDirectory);
            runtimeContext.setContextURL("");
            runtimeContext.setLogLevel(Level.DEBUG);
            runtimeContext.setJobType(JobType.ETL_GRAPH);
            runtimeContext.setValidateRequiredParameters(false);
            //graph.checkConfig(null);
		
            if (!graph.isInitialized()) {
			    EngineInitializer.initGraph(graph);
    		}

            IThreadManager threadManager = new SimpleThreadManager();
            WatchDog watchDog = new WatchDog(graph, runtimeContext);

            threadManager.initWatchDog(watchDog);
            WatchDogFuture watchDogFuture = threadManager.executeWatchDog(watchDog);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
	public static void main(String args[]) {
        System.out.println("Here we go!");

        PythonInterpreter pi = new PythonInterpreter();
        pi.set("ia", new PyInteger(42));
        pi.exec("square = ia*ia");
        pi.exec("print 'square1: ' + str(square)");
        PyInteger square = (PyInteger)pi.get("square");
        System.out.println("square: " + square.asInt());

        // graph
        runGraph();
    }
}
