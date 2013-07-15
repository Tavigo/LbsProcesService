java  -cp $STORM_PATH/lib/*:$STORM_PATH/storm-0.8.2.jar:\
target/LbsProcesService-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
com.kiribuki.processervice.TopologyMain ProvaSignal 127.0.0.1;

# -Djava.net.preferIPv4Stack=true
