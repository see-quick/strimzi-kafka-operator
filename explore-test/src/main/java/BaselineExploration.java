import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Baseline exploration, which provides predefined story. Story consist of 1 to n steps, which are needed to execute and
 * verify. Moreover, you can define your own story to test.
 */
public class BaselineExploration extends Exploration {

    private final Logger LOGGER = LogManager.getLogger(BaselineExploration.class);
    private final Scanner userInput = new Scanner(System.in);

    @Override
    public void explore() {
        LOGGER.info("============== BASELINE EXPLORATION ==============");

        List<String> baselineStory = prepareBaselineStory();

        for (int i  = 0;  i < baselineStory.size(); i++) {
            LOGGER.info(i  +  ". Deploying " + baselineStory.get(i));
            decisionList.add(Choices.valueOf(baselineStory.get(i)));

            // TODO: deploy specific component like in systemtest

            LOGGER.info("Is [" + baselineStory.get(i) + "] running? [y/n]");

            String input = userInput.next();

            if (input.equalsIgnoreCase("y")) {
                successPaths.add(new ArrayList<>(decisionList));
            } else if (input.equalsIgnoreCase("n")) {
                wrongPaths.add(new ArrayList<>(decisionList));
            }
        }

        printResults();
        // TODO: make is 1:1, which our bash, which J.S  did.
    }

    private List<String> prepareBaselineStory() {
        List<String> baselineStory  = new ArrayList<>();

        baselineStory.add("ClusterOperator");
        baselineStory.add("Kafka");
        baselineStory.add("KafkaUser");
        baselineStory.add("KafkaTopic");
        baselineStory.add("KafkaConnect");
        baselineStory.add("KafkaConnectS2I");

        return baselineStory;
    }
}
