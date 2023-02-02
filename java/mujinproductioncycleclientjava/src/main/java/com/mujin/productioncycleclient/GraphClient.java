package com.mujin.productioncycleclient;

import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URI;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketFrame;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketFactory;

public class GraphClient {

    private URL _url = null; // passed in url of mujin controller
    private URL _graphEndpoint = null; // URL to http GraphQL endpoint on Mujin controller

    private Map<String, String> _headers = null; // request headers information
    private Map<String, String> _cookies = null; // request cookies information

    public Map<String, Object> robotBridgeState = null; // storing last received RobotBridgesState from subscription

    private static final Logger log = Logger.getLogger(GraphClient.class.getName());

    public GraphClient(String url, String username, String password) throws Exception {
        this._url = new URL(url);
        this._graphEndpoint = new URL(
                this._url.getProtocol(),
                this._url.getHost(),
                this._url.getPort(),
                "/api/v2/graphql",
                null);

        byte[] encodedUsernamePassword = Base64.getEncoder().encode((username + ":" + password).getBytes());
        this._headers = Map.of(
                "Content-Type", "application/json",
                "Accept", "application/json",
                "X-CSRFToken", "token",
                "Authorization", "Basic " + new String(encodedUsernamePassword));
        this._cookies = Map.of(
                "csrftoken", "token");
    }

    /**
     * Returns the received IO values from Mujin controller state
     * 
     * @return A map received IO values
     */
    public Map<String, Object> GetReceivedIOMap() {
        if (this.robotBridgeState == null) {
            return new HashMap<>();
        }
        // convert to the required format
        Map<String, Object> result = new HashMap<String, Object>();
        Object receivedIoValuesObject = this.robotBridgeState.getOrDefault("receivediovalues", new ArrayList<>());
        List<List<Object>> receivedIoValues = (List<List<Object>>) receivedIoValuesObject;
        for (List<Object> ioValue : receivedIoValues) {
            String key = (String) ioValue.get(0);
            Object value = ioValue.get(1);
            result.put(key, value);
        }
        return result;
    }

    /**
     * Returns the sent IO values from Mujin controller state
     * 
     * @return A map sent IO values
     */
    public Map<String, Object> GetSentIOMap() {
        if (this.robotBridgeState == null) {
            return new HashMap<>();
        }
        // convert to the required format
        Map<String, Object> result = new HashMap<String, Object>();
        Object sentIoValuesObject = this.robotBridgeState.getOrDefault("sentiovalues", new ArrayList<>());
        List<List<Object>> sentIoValues = (List<List<Object>>) sentIoValuesObject;
        for (List<Object> ioValue : sentIoValues) {
            String key = (String) ioValue.get(0);
            Object value = ioValue.get(1);
            result.put(key, value);
        }
        return result;
    }

    /**
     * Subscribes to IO changes on Mujin controller.
     * 
     * @throws Exception If subscription fails
     */
    public void SubscribeRobotBridgesState() throws Exception {
        // subscribes to IO changes on Mujin controller.
        String query = """
                    subscription {
                        SubscribeRobotBridgesState {
                            sentiovalues
                            receivediovalues
                        }
                    }
                """;

        // replace the url protocol
        URI websocketUri = this._graphEndpoint.toURI();
        websocketUri = new URI(
                "ws",
                websocketUri.getUserInfo(),
                websocketUri.getHost(),
                websocketUri.getPort(),
                websocketUri.getPath(),
                websocketUri.getQuery(),
                websocketUri.getFragment());

        // create the client for executing the subscription
        WebSocket webSocket = new WebSocketFactory().createSocket(websocketUri);
        // add the headers
        this._headers.forEach((key, value) -> {
            webSocket.addHeader(key, value);
        });
        // add the listener
        webSocket.addListener(new WebSocketAdapter() {
            @Override
            public void onConnected(WebSocket webSocket, Map<String, List<String>> headers) throws Exception {
                // send the WebSocket connection initialization request
                JSONObject init = new JSONObject();
                init.put("type", "connection_init");
                init.put("payload", new JSONObject());
                webSocket.sendText(init.toString());

                // start a new subscription on the WebSocket connection
                JSONObject payload = new JSONObject();
                payload.put("query", query);
                JSONObject start = new JSONObject();
                start.put("type", "start");
                start.put("payload", payload);
                webSocket.sendText(start.toString());
            }

            @Override
            public void onTextMessage(WebSocket webSocket, String message) throws Exception {
                // read incoming messages
                JSONObject response = new JSONObject(message);
                if (response.getString("type").equals("connection_ack")) {
                    log.info("Received connection_ack");
                } else if (response.getString("type").equals("connection_ack")) {
                    // received keep-alive "ka" message
                } else {
                    // update with response robotBridgeState
                    GraphClient.this.robotBridgeState = response
                            .getJSONObject("payload")
                            .getJSONObject("data")
                            .getJSONObject("SubscribeRobotBridgesState")
                            .toMap();
                }
            }

            @Override
            public void onDisconnected(WebSocket webSocket, WebSocketFrame serverCloseFrame,
                    WebSocketFrame clientCloseFrame, boolean closedByServer) throws Exception {
                log.info("Disconnected from the server");
            }
        }).connect();
    }

    /**
     * Sends GraphQL query to set IO variables to Mujin controller.
     * 
     * @param ioNameValues List of Map<ioName, ioValue> for IO variables to set
     * @throws Exception If cannot set IO variables
     */
    public void SetControllerIOVariables(List<Map<String, Object>> ioNameValues) throws Exception {
        String query = "mutation SetControllerIOVariables($parameters: Any!) {\n"
                + " CommandRobotBridges(command: \"SetControllerIOVariables\", parameters: $parameters)\n"
                + "}";

        // convert to the required format
        List<List<Object>> values = new ArrayList<>();
        for (Map<String, Object> ioNameValue : ioNameValues) {
            for (Map.Entry<String, Object> value : ioNameValue.entrySet()) {
                values.add(Arrays.asList(value.getKey(), value.getValue()));
            }
        }

        // prepare the request body
        JSONObject parameters = new JSONObject();
        parameters.put("ioNameValues", values);
        JSONObject variables = new JSONObject();
        variables.put("parameters", parameters);
        JSONObject data = new JSONObject();
        data.put("query", query);
        data.put("variables", variables);

        // create a client
        HttpURLConnection connection = (HttpURLConnection) this._graphEndpoint.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");

        // add the headers
        this._headers.forEach((key, value) -> {
            connection.setRequestProperty(key, value);
        });

        // add the cookies
        this._cookies.forEach((key, value) -> {
            connection.setRequestProperty("Cookie", key + "=" + value);
        });

        // write the body
        connection.setDoOutput(true);
        OutputStream stream = connection.getOutputStream();
        stream.write(data.toString().getBytes());
        stream.flush();
        stream.close();

        // read the response
        BufferedReader streamReader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
        StringBuilder bodyBuilder = new StringBuilder();
        String value;
        while ((value = streamReader.readLine()) != null) {
            bodyBuilder.append(value);
        }
        String body = bodyBuilder.toString();

        // parse the response
        JSONObject response = new JSONObject(body);
        if (response.has("errors")) {
            throw new Exception(
                    "Failed to set io variables for " + ioNameValues + ". response: " + body);
        }
    }

    /**
     * Sends GraphQL query to get single IO variable from Mujin controller
     * 
     * @param ioName Name of IO variable to get
     * @return Value of IO variable
     * @throws Exception If cannot get the IO value
     */
    public Object GetControllerIOVariable(String ioName) throws Exception {
        String query = "mutation GetControllerIOVariable($parameters: Any!) {\n"
                + "  CommandRobotBridges(command: \"GetControllerIOVariable\", parameters: $parameters)\n"
                + "}";

        // prepare the request body
        JSONObject parameters = new JSONObject();
        parameters.put("parametername", ioName);
        JSONObject variables = new JSONObject();
        variables.put("parameters", parameters);
        JSONObject data = new JSONObject();
        data.put("query", query);
        data.put("variables", variables);

        // create a client
        HttpURLConnection connection = (HttpURLConnection) this._graphEndpoint.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");

        // add the headers
        this._headers.forEach((key, value) -> {
            connection.setRequestProperty(key, value);
        });

        // add the cookies
        this._cookies.forEach((key, value) -> {
            connection.setRequestProperty("Cookie", key + "=" + value);
        });

        // write the body
        connection.setDoOutput(true);
        OutputStream stream = connection.getOutputStream();
        stream.write(data.toString().getBytes());
        stream.flush();
        stream.close();

        // read the response
        BufferedReader streamReader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
        StringBuilder bodyBuilder = new StringBuilder();
        String value;
        while ((value = streamReader.readLine()) != null) {
            bodyBuilder.append(value);
        }
        String body = bodyBuilder.toString();

        // parse the response
        JSONObject response = new JSONObject(body);
        if (response.has("errors")) {
            throw new Exception(
                    "Failed to get io variables for IO name " + ioName + ". response: " + body);
        }
        JSONObject commandRobotBridges = response.getJSONObject("data").getJSONObject("CommandRobotBridges");
        Object parameterValue = commandRobotBridges.get("parametervalue");
        if (parameterValue == null) {
            throw new Exception(
                    "Failed to get io variables for IO name " + ioName + ". response: " + body);
        }

        return parameterValue;
    }
}
