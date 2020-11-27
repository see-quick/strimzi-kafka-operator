import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;
import java.util.Scanner;

/**
 * CreativeExploration is a dialog application for exploratory testing with Strimzi. It provides following choices @see#Components
 * and also after you are done with exploration it provides results success paths and bugs paths
 * f.e.
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - Bugs paths:
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - [ClusterOperator, Kafka, KafkaUser, KafkaUser, KafkaTopic, KafkaTopic]
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - [ClusterOperator, Kafka, KafkaUser, KafkaUser, KafkaTopic, KafkaTopic, KafkaUser]
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - Success paths:
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - [ClusterOperator]
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - [ClusterOperator, Kafka]
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - [ClusterOperator, Kafka, KafkaUser]
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - [ClusterOperator, Kafka, KafkaUser, KafkaUser]
 * [INFO ] 2020-11-27 15:19:18.773 [main] CreativeExploration - [ClusterOperator, Kafka, KafkaUser, KafkaUser, KafkaTopic]
 */
public class CreativeExploration extends Exploration {

    // TODO: ability go backwards (command pattern)

    private static final Map<Choices, Integer> currentChoices;
    private static final Logger LOGGER = LogManager.getLogger(CreativeExploration.class);

    static {
        // for ordering keys
        currentChoices = new EnumMap<>(Choices.class);
        // entry point
        currentChoices.put(Choices.ClusterOperator, 0);  // first choice
    }

    private final Scanner userInput = new Scanner(System.in);

    /**
     * Hearth logic of CreativeExploration. Infinite loop, which asks you what do you want to deploy and you just type "number"
     * f.e. you can type number '1' and Kafka will be deployed. After you type 1 you need manually verify that specific
     * components is ready and working properly. And application you again ask you if that component is ready and you have to
     * type 'y', 'n' or 'STOP' if you want stop exploration.
     * before:
     * [INFO ] 2020-11-27 15:19:03.603 [main] CreativeExploration - You have following choices with your thoughts:
     * [0] - 'ClusterOperator' :1 deployed
     * [1] - 'Kafka' :0 deployed
     * > 1
     * [INFO ] 2020-11-27 15:19:04.105 [main] CreativeExploration - Is [Kafka] running? [y/n] or you wanna stop testing write STOP
     * > y
     * after:
     * [INFO ] 2020-11-27 15:19:04.646 [main] CreativeExploration - You have following choices with your thoughts:
     * [0] - 'ClusterOperator' :1 deployed
     * [1] - 'Kafka' :1 deployed
     * [2] - 'KafkaUser' :0 deployed
     * [3] - 'KafkaTopic' :0 deployed
     */
    @Override
    public void explore() {
        while (true) {
            getAllPossibleChoices();

            Choices choice = Choices.values()[userInput.nextInt()];

            switch (choice) {
                case ClusterOperator:
                    LOGGER.info("Deploying Cluster Operator.");
                    decisionList.add(Choices.ClusterOperator);

                    // TODO:  deployment from STs

                    if (currentChoices.containsKey(Choices.ClusterOperator)) {
                        currentChoices.put(Choices.ClusterOperator, currentChoices.get(Choices.ClusterOperator) + 1); // increment number of
                    }
                    currentChoices.put(Choices.Kafka, 0);       // adding to choice deploy Kafka
                    break;
                case Kafka:
                    LOGGER.info("Deploying Kafka cluster.");
                    decisionList.add(Choices.Kafka);

                    // TODO:  deployment from STs

                    if (currentChoices.containsKey(Choices.Kafka)) {
                        currentChoices.put(Choices.Kafka, currentChoices.get(Choices.Kafka) + 1); // increment number of
                    }

                    currentChoices.put(Choices.KafkaUser, currentChoices.getOrDefault(Choices.KafkaUser, 0));
                    currentChoices.put(Choices.KafkaTopic, currentChoices.getOrDefault(Choices.KafkaTopic, 0));
                    currentChoices.put(Choices.KafkaConnect, currentChoices.getOrDefault(Choices.KafkaConnect, 0));
                    currentChoices.put(Choices.KafkaConnectS2I, currentChoices.getOrDefault(Choices.KafkaConnectS2I, 0));
                    currentChoices.put(Choices.KafkaStreams, currentChoices.getOrDefault(Choices.KafkaStreams, 0));

                    break;
                case KafkaUser:
                    LOGGER.info("Deploying KafkaUser.");
                    decisionList.add(Choices.KafkaUser);

                    // TODO:  deployment from STs

                    if (currentChoices.containsKey(Choices.KafkaUser)) {
                        currentChoices.put(Choices.KafkaUser, currentChoices.get(Choices.KafkaUser) + 1);
                    }
                    break;
                case KafkaTopic:
                    LOGGER.info("Deploying KafkaTopic.");
                    decisionList.add(Choices.KafkaTopic);

                    // TODO:  deployment from STs

                    if (currentChoices.containsKey(Choices.KafkaTopic)) {
                        currentChoices.put(Choices.KafkaTopic, currentChoices.get(Choices.KafkaTopic) + 1);
                    }
                    break;
                case KafkaStreams:
                    LOGGER.info("Deploying KafkaStreams.");
                    decisionList.add(Choices.KafkaStreams);

                    // TODO:  deployment from STs

                    if (currentChoices.containsKey(Choices.KafkaTopic)) {
                        currentChoices.put(Choices.KafkaStreams, currentChoices.get(Choices.KafkaStreams) + 1);
                    }
                    break;
                case KafkaConnect:
                    LOGGER.info("Deploying KafkaConnect.");
                    decisionList.add(Choices.KafkaConnect);

                    // TODO:  deployment from STs

                    if (currentChoices.containsKey(Choices.KafkaConnect)) {
                        currentChoices.put(Choices.KafkaConnect, currentChoices.get(Choices.KafkaConnect) + 1);
                    }
                    break;
                case KafkaConnectS2I:
                    LOGGER.info("Deploying KafkaConnectS2I.");
                    decisionList.add(Choices.KafkaConnectS2I);

                    // TODO:  deployment from STs

                    if (currentChoices.containsKey(Choices.KafkaConnectS2I)) {
                        currentChoices.put(Choices.KafkaConnectS2I, currentChoices.get(Choices.KafkaConnectS2I) + 1);
                    }
                // TODO: add more choices such as Tracing, Oauth, Clients, Strimzi upgrade, RollingUpdate, Scale-up, Scale-down etc...
            }

            LOGGER.info("Is [" + choice + "] running? [y/n] or you wanna stop testing write STOP");
            String resultFromTester = userInput.next();

            if (resultFromTester.equalsIgnoreCase("n")) {
                LOGGER.error("Hmm, something is wrong...We need to investigate!");
                LOGGER.info("Storing sequence choices, which detect bug:" + decisionList.toString());
                wrongPaths.add(new ArrayList<>(decisionList));  // new for storing different list and avoid storing same item
            } else if (resultFromTester.equalsIgnoreCase("y")) {
                LOGGER.info("Storing successful sequence choices:" + decisionList.toString());
                successPaths.add(new ArrayList<>(decisionList)); // new for storing different list and avoid storing same item
            } else if (resultFromTester.equalsIgnoreCase("STOP")) {
                break;
            }
            LOGGER.info("SUCCESS PATHS:" + successPaths.toString());
            LOGGER.info("Current sequence choices " + showSequenceChoices());
        }

        printResults();
    }

    /**
     * Prints all possible choices in the current state
     * f.e
     * [INFO ] 2020-11-27 15:19:04.646 [main] CreativeExploration - You have following choices with your thoughts:
     * [0] - 'ClusterOperator' :1 deployed
     * [1] - 'Kafka' :1 deployed
     * [2] - 'KafkaUser' :0 deployed
     * [3] - 'KafkaTopic' :0 deployed
     */
    private void getAllPossibleChoices() {
        LOGGER.info("You have following choices with your thoughts:");

        int selections = 0;
        for (Map.Entry<Choices, Integer> entry : currentChoices.entrySet()) {
            System.out.println("[" + selections  + "] - '" + entry.getKey() + "' :" + entry.getValue() + " deployed");
            selections++;
        }
    }

    /**
     * Prints sequence of choices for specific turn
     * f.e.
     * [INFO ] 2020-11-27 17:13:10.173 [main] CreativeExploration - Current sequence choices 0.ClusterOperator -> 1.Kafka
     */
    private String showSequenceChoices() {
        StringBuilder sequenceChoices = new StringBuilder();

        for (int i = 0; i < decisionList.size(); i++) {
            sequenceChoices.append(i);
            sequenceChoices.append(".");
            sequenceChoices.append(decisionList.get(i));
            sequenceChoices.append(" -> ");
        }
        return sequenceChoices.toString();
    }
}

