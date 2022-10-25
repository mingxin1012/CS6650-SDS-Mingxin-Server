package com.bsds;


import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(
        name = "SkierServlet",
        value = {"/SkierServlet"}
)
public class SkierServlet extends HttpServlet {
    private final static String RABBITMQ_URL = "localhost";
    private final static Integer MAX_CHANNEL_POOL_SIZE = 100;
    private final static String QUEUE_NAME = "skiersQueue";
    private RMQChannelPool rmqChannelPool;

    @Override
    public void init() throws ServletException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(RABBITMQ_URL);
        RMQChannelFactory rmqChannelFactory;
        try {
            rmqChannelFactory = new RMQChannelFactory(connectionFactory.newConnection());
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Error: failed to create new connection. " + e);
        }
        this.rmqChannelPool = new RMQChannelPool(MAX_CHANNEL_POOL_SIZE, rmqChannelFactory);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/plain");
        String urlPath = request.getPathInfo();
        if (urlPath != null && !urlPath.isEmpty()) {
            String[] urlParts = urlPath.split("/");
            if (!this.isUrlValid(urlParts)) {
                response.setStatus(400);
                response.getWriter().write("Invalid URL");
            } else {
                response.setStatus(200);
                response.getWriter().write("It works!");
            }

        } else {
            response.setStatus(404);
            response.getWriter().write("missing parameters");
        }
    }

    private boolean isUrlValid(String[] urlPath) {
        if (urlPath.length == 3) {
            return urlPath[0].equals("skiers") && urlPath[2].equals("vertical");
        } else if (urlPath.length != 8) {
            return false;
        } else {
            return urlPath[0].equals("skiers") && urlPath[2].equals("seasons") && urlPath[4].equals("days") && urlPath[6].equals("skiers");
        }
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String payload;
        PrintWriter out = response.getWriter();
        payload = this.processData(request, response);
        this.publish(payload);
        response.setStatus(200);
        out.println("<h1>successfully add!</h1>");
        out.flush();
    }

    protected String processData(HttpServletRequest request, HttpServletResponse response) {
        response.setContentType("application/json");

        // format Json Payload
        StringBuilder body = new StringBuilder();
        String line;
        BufferedReader reader = null;
        try {
            reader = request.getReader();
            while ((line = reader.readLine()) != null)
                body.append(line);
        } catch (IOException e) {
            response.setStatus(404);
            throw new RuntimeException("Error: failed to process the request. " + e);
        }
        return request.getPathInfo() + "/body/" + body.toString();

    }
    //publish payload to RabbitMQ
    protected void publish(String message){

        Channel channel = this.rmqChannelPool.borrowObject();
        try {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("message publish to RMQ " + message);
        } catch (IOException e) {
            throw new RuntimeException("Error: failed to operation on channel. " + e);
        }
        this.rmqChannelPool.returnObject(channel);
        System.out.println(" [x] Sent '" + message + "'");
    }

}