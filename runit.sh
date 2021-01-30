#!/bin/bash


for i in /home/daniel/src/jython-test/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$i"
done;

#CLASSPATH=/home/daniel/src/jython-test/lib/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar
#CLASSPATH=$CLASSPATH:/home/daniel/src/jython-test/lib/jython-standalone-2.7.2.jar
#CLASSPATH=$CLASSPATH:/home/daniel/src/jython-test/lib/cloveretl.engine.jar

#java -cp $CLASSPATH org.python.util.jython test.py
#java -cp $CLASSPATH org.python.util.jython functest.py
javac -cp $CLASSPATH Test.java
java -cp $CLASSPATH Test

#java -cp $CLASSPATH org.python.util.jython funcdebug.py
#java -cp $CLASSPATH org.python.util.jython --version

