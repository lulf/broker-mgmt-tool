import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonLinkOptions;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.message.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: Description
 */
public class TopicTester {
    public static void main(String [] args) throws Exception {
        if (args.length < 4) {
            throw new IllegalArgumentException("Usage: tester <pub|sub|mgmt> <host> <port> (<address> | <mgmtcmd ...>)");
        }
        String cmd = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);

        if (cmd.equals("sub")) {
            String address = args[3];
            startReceiver(address, host, port);
        } else if (cmd.equals("pub")) {
            String address = args[3];
            startSender(address, host, port);
        } else if (cmd.equals("mgmt")) {
            String op = args[3];
            String mgmtCmd = args[4];
            List<String> rest = new ArrayList<>();
            for (int i = 5; i < args.length; i++) {
                rest.add(args[i]);
            }
            runMgmtCommand(host, port, op, mgmtCmd, rest.toArray(new Object[0]));
        } else if (cmd.equals("connstart")) {
            Map<String, Object> params = new HashMap<>();
            params.put("host", "127.0.0.1");
            params.put("port", "12345");
            runMgmtCommand(host, port, "core.server", "createConnectorService", "testconnector", "org.apache.activemq.artemis.integration.amqp.AMQPConnectorServiceFactory", params);
        } else if (cmd.equals("connend")) {
            runMgmtCommand(host, port, "core.server", "destroyConnectorService", "testconnector");
        } else {
            throw new IllegalArgumentException("Invalid command " + cmd);
        }
    }

    private static void runMgmtCommand(String host, int port, String op, String cmd, Object ... args) throws Exception {
        ServerLocator locator = ActiveMQClient.createServerLocator(String.format("tcp://%s:%d", host, port));
        ClientSessionFactory sessionFactory = locator.createSessionFactory();
        ClientSession session = sessionFactory.createSession();

        ClientRequestor requestor = new ClientRequestor(session, "activemq.management");
        ClientMessage message = session.createMessage(false);
        ManagementHelper.putOperationInvocation(message, op, cmd, args);
        session.start();
        ClientMessage reply = requestor.request(message);
        Object retVal = ManagementHelper.getResult(reply);
        session.stop();
        if (retVal instanceof Object[]) {
            Object[] vals = (Object[]) retVal;
            for (Object obj: vals) {
                System.out.println(obj.toString());
            }
        } else {
            if (retVal != null) {
                System.out.println(retVal.toString());
            }
        }
    }

    public static void startSender(String address, String host, int port) {
        Vertx vertx = Vertx.vertx();
        String containerId = "topic-sender";
        ProtonClient client = ProtonClient.create(vertx);

        client.connect(host, port, connection -> {
            if (connection.succeeded()) {
                ProtonConnection conn = connection.result();
                conn.setContainer(containerId);
                conn.closeHandler(res -> {
                    System.out.println("Publisher connection closed");
                });
                conn.openHandler(result -> {
                    System.out.println("Connected: " + result.result().getRemoteContainer());
                    Target target = new Target();
                    target.setAddress(address);
                    target.setCapabilities(Symbol.getSymbol("topic"));
                    target.setDurable(TerminusDurability.UNSETTLED_STATE);
                    ProtonSender sender = conn.createSender(address, new ProtonLinkOptions().setLinkName(containerId));
                    sender.setTarget(target);
                    sender.openHandler(res -> {
                        if (res.succeeded()) {
                            System.out.println("Opened publisher");
                        } else {
                            System.out.println("Failed opening publisher: " + res.cause().getMessage());
                        }
                    });
                    sender.closeHandler(res -> {
                        System.out.println("Publisher closed");
                        conn.close();
                    });
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    startSendMessage(vertx, address, reader, sender);
                    sender.open();
                });
                conn.open();
            } else {
                System.out.println("Publisher connection failed: " + connection.cause().getMessage());
            }
        });
    }

    private static void startSendMessage(Vertx vertx, String address, BufferedReader reader, ProtonSender sender) {
        vertx.executeBlocking(future -> {
            try {
                future.complete(reader.readLine());
            } catch (IOException e) {
                future.fail(e);
            }
        }, result -> {
            if (result.succeeded()) {
                Message message = Message.Factory.create();
                message.setAddress(address);
                message.setBody(new AmqpValue(result.result()));
                sender.send(message);
            } else {
                System.out.println("Error reading input: " + result.cause().getMessage());
            }
            startSendMessage(vertx, address, reader, sender);
        });
    }

    public static void startReceiver(String address, String host, int port) {
        Vertx vertx = Vertx.vertx();
        String containerId = "topic-receiver";
        ProtonClient client = ProtonClient.create(vertx);
        client.connect(host, port, connection -> {
            if (connection.succeeded()) {
                ProtonConnection conn = connection.result();
                conn.setContainer(containerId);
                conn.closeHandler(res -> {
                    System.out.println("Subscriber connection closed");
                });
                conn.openHandler(result -> {
                    System.out.println("Connected: " + result.result().getRemoteContainer());
                    Source source = new Source();
                    source.setAddress(address);
                    source.setCapabilities(Symbol.getSymbol("topic"));
                    source.setDurable(TerminusDurability.UNSETTLED_STATE);
                    ProtonReceiver receiver = conn.createReceiver(address, new ProtonLinkOptions().setLinkName(containerId));
                    receiver.setSource(source);
                    receiver.openHandler(res -> {
                        if (res.succeeded()) {
                            System.out.println("Opened subscriber");
                        } else {
                            System.out.println("Failed opening subscriber: " + res.cause().getMessage());
                        }
                    });
                    receiver.closeHandler(res -> {
                        System.out.println("Subscriber closed");
                        conn.close();
                    });
                    receiver.handler((delivery, message) -> System.out.println("GOT MESSAGE: " + message.getBody().toString()));
                    receiver.open();
                });
                conn.open();
            } else {
                System.out.println("Subscriber connection failed: " + connection.cause().getMessage());
            }
        });
    }
}
