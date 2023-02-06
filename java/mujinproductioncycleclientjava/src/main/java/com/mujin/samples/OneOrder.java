package com.mujin.samples;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
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

    private List<Map<String, Object>> _orderResults = Collections.synchronizedList(new ArrayList<>());
    
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

        // pick and place locations in the robot cell
        final String sourceContainerId = "source0001"; // unique id of the source container, usually barcode of the box, or agv id, must not be constant when pick container changes
        final String destinationContainerId = "dest0001"; // unique id of the destination pallet, usually barcode of the pallet, must not be constant when place contianer changes

        // handle location move in and out for location 1
        CompletableFuture<Void> handlePickLocationMove = CompletableFuture.runAsync(() -> this.HandleLocationMove(
                graphClient,
                "location1",
                sourceContainerId, // use containerId matching the queued order request
                "location1ContainerId", // location1 here is example, depend on Mujin controller configuration
                "location1HasContainer", // location1 here is example, depend on Mujin controller configuration
                "moveInLocation1Container", // location1 here is example, depend on Mujin controller configuration
                "moveOutLocation1Container") // location1 here is example, depend on Mujin controller configuration
        );

        // handle location move in and out for location 2
        CompletableFuture<Void> handlePlaceLocationMove = CompletableFuture.runAsync(() -> this.HandleLocationMove(
                graphClient,
                "location2",
                destinationContainerId, // use containerId matching the queued order request
                "location2ContainerId", // location2 here is example, depend on Mujin controller configuration
                "location2HasContainer", // location2 here is example, depend on Mujin controller configuration
                "moveInLocation2Container", // location2 here is example, depend on Mujin controller configuration
                "moveOutLocation2Container") // location2 here is example, depend on Mujin controller configuration
        ); 

        // dequeue order results
        CompletableFuture<Void> dequeueOrderResults = CompletableFuture.runAsync(() -> this.DequeueOrderResults(orderManager));


        // 
        // 1. Execute Depalletizing
        // 

        // queue a depalletizing order to productionQueue1Order
        Map<String, Object> depalletizingOrderEntry = Map.ofEntries(
            entry("orderUniqueId", "order_0001"), // unique id for this order
            entry("orderGroupId", "group_0001"), // group multiple orders to same place container
            entry("orderPickContainerId", sourceContainerId),
            entry("orderPlaceContainerId", ""),
            entry("orderScenarioId", "depallet"),
            entry("orderType", "picking"),
            entry("orderNumber", 100), // number of parts to pick
            entry("orderInputPartIndex", 0), // 1-based index into the pack formation, 1 meaning the first box in the pack
            entry("orderPickLocationName", "location1"),
            entry("orderPlaceLocationName", "location2"),
            entry("orderPartWeight", 0),
            entry("orderPartSizeX", 0),
            entry("orderPartSizeY", 0),
            entry("orderPartSizeZ", 0)
            // NOTE: additional parameters may be required depending on the configurations on Mujin controller
        );
        orderManager.QueueOrder(depalletizingOrderEntry);
        log.info("Queued a depalletizing order: " + depalletizingOrderEntry.toString());


        // 
        // 2. Receive Depalletizing Result
        // 

        // receive the result from productionQueue1Result 
        Map<String, Object> depalletizingOrderResult = this.WaitForOrderResult();
        log.info("Received depalletizing order result: " + depalletizingOrderResult.toString());


        // 
        // 3. Request Pack Formation
        // 

        // set inputPackFormationHeader
        Map<String, Object> inputPackFormationHeader = Map.ofEntries(
            entry("inputPackFormationHeader", Map.ofEntries(
                entry("packingUniqueId", "A0"),
                entry("numPacked", 3)
            ))
        );
        graphClient.SetControllerIOVariables(inputPackFormationHeader);
        log.info("Set inputPackFormationHeader to: " + inputPackFormationHeader.toString());

        // set inputPackFormationEntry
        Map<String, Object> inputPackFormationEntry = Map.ofEntries(
            entry("inputPackFormationEntry", List.of(
                Map.ofEntries(
                    entry("partFullSize", List.of(204, 223, 191)),
                    entry("partWeight", 300),
                    entry("aabbPoseInContainer", List.of(1, 0, 0, 0, 0, 0, 0))
                ),
                Map.ofEntries(
                    entry("partFullSize", List.of(204, 223, 193)),
                    entry("partWeight", 200),
                    entry("aabbPoseInContainer", List.of(1, 0, 0, 0, 0, 0, 0))
                ),
                Map.ofEntries(
                    entry("partFullSize", List.of(258, 363, 182)),
                    entry("partWeight", 400),
                    entry("aabbPoseInContainer", List.of(1, 0, 0, 0, 0, 0, 0))
                )
            ))
        );
        graphClient.SetControllerIOVariables(inputPackFormationEntry);
        log.info("Set inputPackFormationEntry to: " + inputPackFormationEntry.toString());
        
        // queue a pack formation computation order to productionQueue1Order
        Map<String, Object> packFormationRequestOrderEntry = Map.ofEntries(
            entry("orderUniqueId", "order_0002"), // unique id for this order
            entry("orderGroupId", "group_0002"), // group multiple orders to same place container
            entry("orderPickContainerId", ""),
            entry("orderPlaceContainerId", ""),
            entry("orderScenarioId", "pack"),
            entry("orderType", "packFormation"),
            entry("orderNumber", 3), // number of parts to pack
            entry("orderInputPartIndex", 0), // 1-based index into the pack formation, 1 meaning the first box in the pack
            entry("orderPickLocationName", "location2"),
            entry("orderPlaceLocationName", "location1"),
            entry("orderPartWeight", 0),
            entry("orderPartSizeX", 0),
            entry("orderPartSizeY", 0),
            entry("orderPartSizeZ", 0)
            // NOTE: additional parameters may be required depending on the configurations on Mujin controller
        );
        orderManager.QueueOrder(packFormationRequestOrderEntry);
        log.info("Queued a pack formation request order: " + packFormationRequestOrderEntry.toString());


        // 
        // 4. Receive Pack Formation Result        
        // 

        // read resultPackFormationHeader
        Map<String, Object> resultPackFormationHeader = (Map<String, Object>)graphClient.GetControllerIOVariable("resultPackFormationHeader");
        log.info("Read pack formation result header: " + resultPackFormationHeader.toString());

        // read resultPackFormationEntry
        Map<String, Object> resultPackFormationEntry = (Map<String, Object>)graphClient.GetControllerIOVariable("resultPackFormationEntry");
        log.info("Read pack formation result entry: " + resultPackFormationEntry.toString());

        // receive the order result from productionQueue1Result
        Map<String, Object> packFormationRequestOrderResult = this.WaitForOrderResult();
        log.info("Received pack formation request order result: " + packFormationRequestOrderResult.toString());

        
        // 
        // 5. Execute Pack Formation
        // 

        // set orderPackFormationHeader
        Map<String, Object> orderPackFormationHeader = (Map<String, Object>)resultPackFormationHeader.get("resultPackFormationHeader");
        graphClient.SetControllerIOVariables(orderPackFormationHeader);
        log.info("Set orderPackFormationHeader to: " + orderPackFormationHeader.toString());

        // set orderPackFormationEntry
        Map<String, Object> orderPackFormationEntry = (Map<String, Object>)resultPackFormationEntry.get("resultPackFormationEntry");
        graphClient.SetControllerIOVariables(orderPackFormationEntry);
        log.info("Set orderPackFormationEntry to: " + orderPackFormationEntry.toString());

        // queue a pack formation execution order
        Map<String, Object> packFormationExecutionOrderEntry = Map.ofEntries(
            entry("orderUniqueId", "order_0003"), // unique id for this order
            entry("orderGroupId", "group_0003"), // group multiple orders to same place container
            entry("orderPickContainerId", sourceContainerId),
            entry("orderPlaceContainerId", destinationContainerId),
            entry("orderScenarioId", "pallet"),
            entry("orderType", "picking"),
            entry("orderNumber", 1), // number of parts to pick
            entry("orderInputPartIndex", 1), // 1-based index into the pack formation, 1 meaning the first box in the pack
            entry("orderPickLocationName", "location2"),
            entry("orderPlaceLocationName", "location1"),
            entry("orderPartWeight", 300.0),
            entry("orderPartSizeX", 204.0),
            entry("orderPartSizeY", 223.0),
            entry("orderPartSizeZ", 191.0)
            // NOTE: additional parameters may be required depending on the configurations on Mujin controller
        );
        orderManager.QueueOrder(packFormationExecutionOrderEntry);
        log.info("Queued a pack formation execution order: " + packFormationExecutionOrderEntry.toString());

        // receive the result from productionQueue1Result 
        Map<String, Object> packFormationExecutionOrderResult = this.WaitForOrderResult();
        log.info("Received pack formation execution order result: " + packFormationExecutionOrderResult.toString());

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
                    this._orderResults.add(resultEntry);
                }
            } catch (Exception e) {
                log.warning("Failed to dequeue order result: " + e.toString());
            }
        }
    }

    /**
     * Blocks until an order result is ready
     * 
     * @return Order result
     */
    public Map<String, Object> WaitForOrderResult() {
        Map<String, Object> orderResult = null;
        while (orderResult == null) {
            // wait for the depalletizing order result
            synchronized (this._orderResults) {
                int orderResultsSize = this._orderResults.size();
                if (orderResultsSize > 0) {
                    orderResult = this._orderResults.remove(this._orderResults.size() - 1);
                }
            }
        }
        return orderResult;
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
