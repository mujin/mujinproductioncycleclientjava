package com.mujin.samples;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.concurrent.CompletableFuture;

import static java.util.Map.entry;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import com.mujin.productioncycleclient.GraphClient;
import com.mujin.productioncycleclient.OrderManager;

public class OneOrder {

    private static final Logger log = Logger.getLogger(OneOrder.class.getName());

    private void _RunMain(String url, String username, String password) throws Exception {
        // GraphQLClient to set and get controller io variables
        GraphClient graphClient = new GraphClient(url, username, password);

        CompletableFuture.runAsync(() -> this._SubscribeRobotBridgesState(graphClient));
        this._ManageProductionCycle(graphClient);
    }

    private void _SubscribeRobotBridgesState(GraphClient graphClient) {
        try {
            graphClient.SubscribeRobotBridgesState();
        } catch (Exception e) {
            log.warning("Graphql subscription failed: " + e.toString());
        }
    }

    /**
     * Starts production cycle and queues a single order. Manages the location
     * states for the order to be processed and dequeue the order result.
     * 
     * @param graphClient For checking Mujin IO state and setting IO
     * @throws Exception
     */
    private void _ManageProductionCycle(GraphClient graphClient) throws Exception {
        // ProductionCycleOrderManager to manage order pointers, queue orders, and read order results
        OrderManager orderManager = new OrderManager(graphClient, 1);

        // initialize internal order queue pointers
        orderManager.InitializeOrderPointers(5);

        // start production cycle
        this.StartProductionCycle(graphClient);

        // queue an order
        String pickLocationName = "location1"; // name of the pick location set up in Mujin controller
        String placeLocationName = "location2"; // name of the place location set up in Mujin controller
        String pickContainerId = "source0001"; // unique id of the source container, usually barcode of the box, or agv id, must not be constant when pick container changes
        String placeContainerId = "dest0001"; // unique id of the destination pallet, usually barcode of the pallet, must not be constant when place contianer changes

        Map<String, Object> orderEntry = Map.ofEntries(
                entry("orderUniqueId", "order" + (int) (System.currentTimeMillis() / 1000)), // unique id for this order
                entry("orderGroupId", "group1"), // group multiple orders to same place container
                entry("orderNumber", 100), // number of parts to pick
                entry("orderPartSizeX", 300),
                entry("orderPartSizeY", 450),
                entry("orderPartSizeZ", 250),
                entry("orderInputPartIndex", 0), // 1-based index into the pack formation, 1 meaning the first box in the pack
                entry("orderPickContainerId", pickContainerId),
                entry("orderPlaceContainerId", placeContainerId),
                entry("orderPickLocationName", pickLocationName),
                entry("orderPlaceLocationName", placeLocationName),
                entry("orderScenarioId", "depallet"),
                entry("orderType", "picking")
                // NOTE: additional parameters may be required depending on the configurations on Mujin controller
        );
        orderManager.QueueOrder(orderEntry);
        log.info("Queued order: " + orderEntry.toString());

        // handle location move in and out for source location
        CompletableFuture<Void> handlePickLocationMove = CompletableFuture.runAsync(() -> this.HandleLocationMove(
                graphClient,
                pickLocationName,
                pickContainerId, // use containerId matching the queued order request
                "location1ContainerId", // location1 here is example, depend on Mujin controller configuration
                "location1HasContainer", // location1 here is example, depend on Mujin controller configuration
                "moveInLocation1Container", // location1 here is example, depend on Mujin controller configuration
                "moveOutLocation1Container") // location1 here is example, depend on Mujin controller configuration
        );

        // handle location move in and out for destination location
        CompletableFuture<Void> handlePlaceLocationMove = CompletableFuture.runAsync(() -> this.HandleLocationMove(
                graphClient,
                placeLocationName,
                placeContainerId, // use containerId matching the queued order request
                "location2ContainerId", // location2 here is example, depend on Mujin controller configuration
                "location2HasContainer", // location2 here is example, depend on Mujin controller configuration
                "moveInLocation2Container", // location2 here is example, depend on Mujin controller configuration
                "moveOutLocation2Container") // location2 here is example, depend on Mujin controller configuration
        ); 

        // dequeue order results
        CompletableFuture<Void> dequeueOrderResults = CompletableFuture.runAsync(() -> this.DequeueOrderResults(orderManager));

        // wait until all operations are complete
        CompletableFuture.allOf(handlePickLocationMove, handlePlaceLocationMove, dequeueOrderResults).get();
    }

    /**
     * Starts production cycle.
     * 
     * @param graphClient For checking Mujin IO state and setting IO
     */
    public void StartProductionCycle(GraphClient graphClient) throws Exception {
        // start production cycle
        if (!(boolean) graphClient.GetSentIOMap().getOrDefault("isRunningProductionCycle", false)) {
            graphClient.SetControllerIOVariables(Map.of("startProductionCycle", true));
        }

        while (!(boolean) graphClient.GetSentIOMap().getOrDefault("isRunningProductionCycle", false)) {
            // wait for production cycle to start running
        }

        // set trigger off
        graphClient.SetControllerIOVariables(Map.of("startProductionCycle", false));

        log.info("Started production cycle");
    }

    /**
     * Dequeues order results in the order result queue.
     * 
     * @param orderManager For dequeuing order results and managing order pointers
     */
    public void DequeueOrderResults(OrderManager orderManager) {
        while (true) {
            try {
                // read the order result
                Map<String, Object> resultEntry = orderManager.DequeueOrderResult();
                if (resultEntry != null) {
                    log.info("Read order result: " + resultEntry.toString());
                }
            } catch (Exception e) {
                log.warning("Failed to dequeue order result: " + e.toString());
            }
        }
    }

    /**
     * Handles state management of a location upon move-in and move-out request sent
     * from Mujin.
     * 
     * @param graphClient        For checking Mujin IO state and setting location state IO
     * @param locationName       Name of this location for printing
     * @param containerId        ID of the container to move in to this location. Should be consistent with the queued order information
     * @param containerIdIOName  IO name used to set this location's container ID value
     * @param hasContainerIOName IO name used to set this location's hasContainer
     * @param moveInIOName       IO name used to get and check for move-in request for this location
     * @param moveOutIOName      IO name used to get and check for move-out request for this location
     */
    public void HandleLocationMove(GraphClient graphClient, String locationName, String containerId, String containerIdIOName, String hasContainerIOName, String moveInIOName, String moveOutIOName) {

        boolean hasContainer = (boolean) graphClient.GetSentIOMap().getOrDefault(hasContainerIOName, false);
        while (true) {
            try {
                Map<String, Object> ioNameValues = new HashMap<String, Object>();
                Boolean isMoveIn = (Boolean) graphClient.GetSentIOMap().getOrDefault(moveInIOName, false);
                Boolean isMoveOut = (Boolean) graphClient.GetSentIOMap().getOrDefault(moveOutIOName, false);

                // handle move in
                if (isMoveIn && !hasContainer) {
                    // set container ID
                    ioNameValues.put(containerIdIOName, containerId);
                    // hasContainer set True
                    ioNameValues.put(hasContainerIOName, true);
                    hasContainer = true;
                    log.info("Moved in container " + containerId + " to location " + locationName);
                }
                // handle move out
                else if (isMoveOut && hasContainer) {
                    // reset container ID
                    ioNameValues.put(containerIdIOName, "");
                    // hasContainer set False
                    ioNameValues.put(hasContainerIOName, false);
                    hasContainer = false;
                    log.info("Moved out container " + containerId + " of location " + locationName);
                }

                // set ioNameValues
                if (ioNameValues.size() > 0) {
                    graphClient.SetControllerIOVariables(ioNameValues);
                }
            } catch (Exception e) {
                log.warning("Failed to handle location move: " + e.toString());
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("OneOrder").build().defaultHelp(true).description("Example code to run one order on production cycle");
        parser.addArgument("--url").setDefault("http://127.0.0.1").help("URL of the controller");
        parser.addArgument("--username").setDefault("mujin").help("Username to login with");
        parser.addArgument("--password").setDefault("mujin").help("Password to login with");
        Namespace arguments = null;
        try {
            arguments = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        try {
            OneOrder oneOrder = new OneOrder();
            oneOrder._RunMain(
                    arguments.getString("url"),
                    arguments.getString("username"),
                    arguments.getString("password"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
