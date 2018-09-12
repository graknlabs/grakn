package ai.grakn.engine.bootup;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageBootupTest {
    @Test
    public void test() {
    Path graknHome = Paths.get("C:\\Users\\Grakn Labs\\Desktop\\grakn-core-1.4.0-SNAPSHOT");
    Path graknProperties = graknHome.resolve("conf").resolve("grakn.properties");
//    System.setProperty("storage.javaopts", "-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");
//    new StorageBootup(new BootupProcessExecutor(), graknHome, graknProperties).startIfNotRunning();
        EngineBootup inst = new EngineBootup(new BootupProcessExecutor(),graknHome, graknProperties);
        inst.startIfNotRunning();
    }
}
