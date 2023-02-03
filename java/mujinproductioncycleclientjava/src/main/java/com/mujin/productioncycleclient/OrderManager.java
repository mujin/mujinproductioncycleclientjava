package com.mujin.productioncycleclient;

import java.util.Map;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.concurrent.TimeUnit;
import static java.util.Map.entry;

public class OrderManager {

    private String _orderQueueIOName = null; // io name of order request queue
    private String _resultQueueIOName = null; // io name of order result queue

    private String _orderReadPointerIOName = null; // io name of order request read pointer
    private String _orderWritePointerIOName = null; // io name of order request write pointer
    private String _resultReadPointerIOName = null; // io name of order result read pointer
    private String _resultWritePointerIOName = null; // io name of order result write pointer

    private int _orderWritePointer = 0; // value of current order request write pointer
    private int _resultReadPointer = 0; // value of current order result write pointer
    private int _queueLength = 0; // length of order request queue

    private GraphClient _graphClient = null; // instance of graphqlclient.GraphClient

    private static final Logger log = Logger.getLogger(OrderManager.class.getName());

    public OrderManager(GraphClient graphClient, int queueIndex) {
        this._graphClient = graphClient;
        this._orderQueueIOName = "productionQueue" + queueIndex + "Order";
        this._resultQueueIOName = "productionQueue" + queueIndex + "Result";
        this._orderReadPointerIOName = "location" + queueIndex + "OrderReadPointer";
        this._orderWritePointerIOName = "location" + queueIndex + "OrderWritePointer";
        this._resultReadPointerIOName = "location" + queueIndex + "OrderResultReadPointer";
        this._resultWritePointerIOName = "location" + queueIndex + "OrderResultWritePointer";
    }

    /**
     * Increments value for an order queue pointer. Wraps around length of order queue.
     * 
     * @param pointerValue Value of order queue pointer to be incremented
     * @return Incremented pointerValue
     */
    private int _IncrementPointer(int pointerValue) {
        pointerValue++;
        if (pointerValue > this._queueLength) {
            pointerValue = 1;
        }
        return pointerValue;
    }

    /**
     * Sends GraphQL query to get order queue pointers and order queue length
     * 
     * @param timeout Number of seconds to wait for the order pointers to be initialized
     * @throws Exception If cannot initialize within the timeout period
     */
    public void InitializeOrderPointers(long timeout) throws Exception {
        long startTime = System.currentTimeMillis();

        // initialize order queue length from order queue
        _queueLength = ((JSONArray) _graphClient.GetControllerIOVariable(_orderQueueIOName)).length();
        log.info("Order queue length is " + _queueLength);

        // initialize order pointers
        boolean initializedOrderPointers = false;
        while (!initializedOrderPointers) {
            Map<String, Object> receivedIOMap = _graphClient.GetReceivedIOMap();

            _orderWritePointer = (int) receivedIOMap.getOrDefault(_orderWritePointerIOName, 0);
            _resultReadPointer = (int) receivedIOMap.getOrDefault(_resultReadPointerIOName, 0);
            int orderReadPointer = (int) receivedIOMap.getOrDefault(_orderReadPointerIOName, 0);
            int resultWritePointer = (int) receivedIOMap.getOrDefault(_resultWritePointerIOName, 0);

            // verify order queue pointer values are valid
            initializedOrderPointers = true;
            for (int pointerValue : new int[] {
                    _orderWritePointer, _resultReadPointer, orderReadPointer, resultWritePointer
            }) {
                if (pointerValue < 1 || pointerValue > _queueLength) {
                    initializedOrderPointers = false;
                    if (System.currentTimeMillis() - startTime > TimeUnit.SECONDS.toMillis(timeout)) {
                        throw new Exception("Production cycle order queue pointers are invalid");
                    }
                }
            }
        }
    }

    /**
     * Queues an order entry to the order queue.
     * 
     * @param orderEntry Order information to queue to the system
     * @throws Exception If cannot queue an order
     */
    public void QueueOrder(Map<String, Object> orderEntry) throws Exception {
        // queue order to next entry in order queue and increment the order write pointer
        int orderReadPointer = (int) this._graphClient.GetReceivedIOMap().getOrDefault(this._orderReadPointerIOName, 0);

        // check if order queue is full
        if (_IncrementPointer(this._orderWritePointer) == orderReadPointer) {
            throw new Exception("Failed to queue new order entry because order queue is full (length=" + this._queueLength + ").");
        }

        // queue order entry and increment order write pointer
        String orderQueueEntryIOName = this._orderQueueIOName + "[" + (this._orderWritePointer - 1) + "]";
        this._orderWritePointer = _IncrementPointer(this._orderWritePointer);

        Map<String, Object> variables = Map.ofEntries(
            entry(orderQueueEntryIOName, orderEntry),
            entry(this._orderWritePointerIOName, this._orderWritePointer));
        this._graphClient.SetControllerIOVariables(variables);
    }

    /**
     * Dequeues next result entry in order result queue.
     * 
     * @return Order result information. Null if there is no result entry to be read.
     * @throws Exception If cannot dequeue an order
     */
    public Map<String, Object> DequeueOrderResult() throws Exception {
        int resultWritePointer = (int) this._graphClient.GetReceivedIOMap().getOrDefault(this._resultWritePointerIOName, 0);

        // reads next order result from order result queue and increment the order result read pointer
        Map<String, Object> resultEntry = null;
        if (this._resultReadPointer != resultWritePointer) {
            String orderResultQueueEntryIOName = this._resultQueueIOName + "[" + (this._resultReadPointer - 1) + "]";
            resultEntry = ((JSONObject) this._graphClient.GetControllerIOVariable(orderResultQueueEntryIOName)).toMap();
            this._resultReadPointer = this._IncrementPointer(this._resultReadPointer);

            Map<String, Object> variables = Map.ofEntries(entry(this._resultReadPointerIOName, this._resultReadPointer));
            this._graphClient.SetControllerIOVariables(variables);
        }
        return resultEntry;
    }
}
