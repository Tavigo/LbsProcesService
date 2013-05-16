java  -cp $STORM_PATH/lib/*:$STORM_PATH/storm-0.8.2.jar:\
target/LbsProcesService-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
com.kiribuki.processervice.TopologyMain MonitorWlan0;

# -Djava.net.preferIPv4Stack=true
