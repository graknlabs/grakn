package ai.grakn.engine;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import ai.grakn.engine.config.GraknEngineConfigTest;
import ai.grakn.engine.controller.AuthControllerTest;
import ai.grakn.engine.controller.ConceptControllerTest;
import ai.grakn.engine.controller.GraqlControllerDELETETest;
import ai.grakn.engine.controller.GraqlControllerGETTest;
import ai.grakn.engine.controller.GraqlControllerPOSTTest;
import ai.grakn.engine.lock.LockProviderTest;

/**
 * Run a selected set of classes/methods as a main program. Helps with debugging tests, especially
 * troubleshooting interdependencies...
 * 
 * @author borislav
 *
 */
public class DebugTestsRunner {

    public static void main(String[] argv) {
        Class<?> [] torun = new Class[] { 
                GraknEngineConfigTest.class,
                AuthControllerTest.class,
                ConceptControllerTest.class,
                GraqlControllerDELETETest.class,
                LockProviderTest.class,
                GraqlControllerPOSTTest.class};
        JUnitCore junit = new JUnitCore();
        Result result = null;
        do {
            result = junit.run(torun);
        } while (result.getFailureCount() == 0 && false);
        System.out.println("Failures " + result.getFailureCount());
        if (result.getFailureCount() > 0) {
            for (Failure failure : result.getFailures()) {
                failure.getException().printStackTrace();
            }
        }
        System.exit(0);
    }
}