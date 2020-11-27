import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public abstract class Exploration {

    private static final Logger LOGGER = LogManager.getLogger(CreativeExploration.class);

    protected final List<List<Choices>> wrongPaths = new ArrayList<>();  // use to store paths, where we detect bug (f.e. CO -> Kafka -> Kafka -> Kafka)
    protected final List<List<Choices>> successPaths = new ArrayList<>();  // use to store paths, where are correct (f.e. CO -> Kafka -> Kafka -> Kafka)
    protected final List<Choices> decisionList = new ArrayList<>();

    abstract void explore();

    protected void printResults() {
        LOGGER.info("=============================");
        LOGGER.info("=============================");
        LOGGER.info("Report from testing");
        LOGGER.info("=============================");
        LOGGER.info("=============================");
        LOGGER.info("Bugs paths:");
        printPaths(wrongPaths);
        LOGGER.info("Success paths:");
        printPaths(successPaths);
    }

    private void printPaths(List<List<Choices>> paths) {
        for (List<Choices> path : paths) {
            LOGGER.info(path.toString());
        }
    }
}
