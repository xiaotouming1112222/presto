# Prestissimo E2E tests

This module runs the e2e tests from the abstract classes defined in 
presto-tests module with Prestissimo workers. The following command
can be used to run all the tests in this module:
```
./mvnw -pl presto-native-tests -Dtest=com.facebook.presto.nativetests.Test* -Duser.timezone=America/Bahia_Banderas -Dhive.security=legacy -DPRESTO_SERVER=${PRESTO_HOME}/presto-native-execution/cmake-build-debug/presto_cpp/main/presto_server -DDATA_DIR=${HIVE_DATA} -DWORKER_COUNT=${WORKER_COUNT}
```
