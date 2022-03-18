package org.kecak.topics.client;

import com.kinnarastudio.commons.Try;
import com.kinnarastudio.commons.jsonstream.JSONStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kecak.topics.client.exception.KecakTopicException;
import org.kecak.topics.client.model.TopicVariables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class TopicClientBuilder {
    public final static String TOPICS_API_CLASSNAME = "com.kinnara.kecakplugins.topics.TopicsApi";

    private String host;
    private String applicationId;
    private String topic;
    private String username;
    private String password;
    private BiFunction<String, TopicVariables, TopicVariables> onMessage;
    private Consumer<Exception> onError;

    private TopicClientBuilder(){}

    public static TopicClientBuilder getInstance(String host, String applicationId, String topic) {
        final TopicClientBuilder instance = new TopicClientBuilder();
        instance.host = host;
        instance.applicationId = applicationId;
        instance.topic = topic;
        return instance;
    }

    /**
     * Set username for basic auth
     *
     * @param username
     * @return
     */
    public TopicClientBuilder setUsername(String username) {
        this.username = username;
        return this;
    }

    /**
     * Set password for basic auth
     *
     * @param password
     * @return
     */
    public TopicClientBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * @param onMessage (String id, TopicVariables variables)
     * @return
     */
    public TopicClientBuilder onMessage(BiFunction<String, TopicVariables, TopicVariables> onMessage) {
        this.onMessage = onMessage;
        return this;
    }

    public TopicClientBuilder onError(Consumer<Exception> onError) {
        this.onError = onError;
        return this;
    }

    public void connect() {
        final String endpoint = host.replaceAll("/$", "") + "/web/json/app/" + applicationId + "/plugin/" + TOPICS_API_CLASSNAME + "/service?topic=" + topic;
        try {
            final HttpClient client = getHttpClient(true);
            final HttpUriRequest request = getHttpRequest(endpoint);
            final HttpResponse response = client.execute(request);
            final int statusCode = getResponseStatus(response);

            if (getStatusGroupCode(statusCode) != 200) {
                throw new KecakTopicException("Response code [" + statusCode + "] is not 200 (Success)");
            }

            final HttpEntity entity = response.getEntity();
            if (entity == null) {
                return;
            }

            JSONArray jsonArray;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                final String responseBody = br.lines().collect(Collectors.joining());
                jsonArray = new JSONArray(responseBody);
            } catch (IOException e) {
                throw new KecakTopicException(e);
            } catch (JSONException e) {
                jsonArray = new JSONArray();
            }

            JSONStream.of(jsonArray, Try.onBiFunction(JSONArray::getJSONObject))
                    .forEach(Try.onConsumer(jsonObject -> {
                        String assignmentId = jsonObject.getString("id");
                        if (onMessage != null) {
                            final TopicVariables exportVariables = onMessage.apply(assignmentId, new TopicVariables());
                            final HttpEntity postEntity = getRequestEntity(exportVariables);
                            final HttpUriRequest postRequest = getHttpRequest(endpoint + "&id=" + assignmentId, postEntity);
                            final HttpResponse postResponse = client.execute(postRequest);

                            final int postResponseStatus = getResponseStatus(postResponse);
                            if (getStatusGroupCode(postResponseStatus) != 200) {
                                throw new KecakTopicException("Response code [" + postResponseStatus + "] is not 200 (Success) for assignment [" + assignmentId + "]");
                            }
                        }
                    }));
        } catch (IOException | KecakTopicException e) {
            if(onError != null) {
                onError.accept(e);
            } else {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param ignoreCertificate
     * @return
     * @throws KecakTopicException
     */
    private HttpClient getHttpClient(boolean ignoreCertificate) throws KecakTopicException {
        try {
            if (ignoreCertificate) {
                SSLContext sslContext = new SSLContextBuilder()
                        .loadTrustMaterial(null, (certificate, authType) -> true).build();
                return HttpClients.custom().setSSLContext(sslContext)
                        .setSSLHostnameVerifier(new NoopHostnameVerifier())
                        .build();
            } else {
                return HttpClientBuilder.create().build();
            }
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new KecakTopicException(e);
        }
    }

    private HttpEntity getRequestEntity(@Nullable final TopicVariables variables) throws JSONException {
        final JSONObject jsonRequestBody = variables == null ? new JSONObject() : new JSONObject(variables.getVariables());
        return new StringEntity(jsonRequestBody.toString(), ContentType.APPLICATION_JSON);
    }

    private HttpUriRequest getHttpRequest(String url) {
        final HttpRequestBase request = new HttpGet(url);
        if(username != null) {
            request.setHeader("Authorization", "Basic " + getBasicAuth(username, password));
        }
        return request;
    }

    private HttpUriRequest getHttpRequest(String url, @Nullable HttpEntity httpEntity) {
        final HttpRequestBase request = new HttpPost(url);

        if(username != null) {
            request.setHeader("Authorization", "Basic " + getBasicAuth(username, password));
        }

        if (httpEntity != null && request instanceof HttpEntityEnclosingRequestBase) {
            ((HttpEntityEnclosingRequestBase) request).setEntity(httpEntity);
        }

        return request;
    }

    private int getStatusGroupCode(int status) {
        return status - (status % 100);
    }

    private int getResponseStatus(@Nonnull HttpResponse response) throws KecakTopicException {
        return Optional.of(response)
                .map(HttpResponse::getStatusLine)
                .map(StatusLine::getStatusCode)
                .orElseThrow(() -> new KecakTopicException("Error getting status code"));
    }

    private String getBasicAuth(String username, String password) {
        return Base64.getEncoder()
                .encodeToString(String.join(":", username, password).getBytes(StandardCharsets.UTF_8));
    }
}
