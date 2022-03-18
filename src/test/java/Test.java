import org.kecak.topics.client.TopicClientBuilder;

public class Test {
    public static void main(String[] args) {
        System.out.println("Start TEST");
        TopicClientBuilder
                .getInstance("http://localhost", "app1", "topic1")
                .onMessage((id, variables) -> {
                    variables.setVariable("status", "done");
                    return variables;
                })
                .connect();
    }
}
