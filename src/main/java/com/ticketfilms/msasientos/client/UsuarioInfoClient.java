package com.ticketfilms.msasientos.client;

import java.time.Duration;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import lombok.extern.slf4j.Slf4j;

// El access token de Cognito no trae el email; el endpoint userInfo sí.
// Si falla devuelve null: reservar no debe caerse por no poder avisar después.
@Slf4j
@Component
public class UsuarioInfoClient {

    private final RestClient restClient;
    private final String userInfoUrl;

    public UsuarioInfoClient(@Value("${cognito.userinfo-url:}") String userInfoUrl) {
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory();
        factory.setReadTimeout(Duration.ofSeconds(5));
        this.restClient = RestClient.builder().requestFactory(factory).build();
        this.userInfoUrl = userInfoUrl;
    }

    public String obtenerEmail(String bearerToken) {
        if (userInfoUrl == null || userInfoUrl.isBlank()) {
            log.warn("cognito.userinfo-url no está configurada, no se puede obtener el email");
            return null;
        }
        try {
            Map<?, ?> info = restClient.get()
                    .uri(userInfoUrl)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                    .retrieve()
                    .body(Map.class);
            Object email = info != null ? info.get("email") : null;
            return email != null ? email.toString() : null;
        } catch (Exception e) {
            log.warn("No se pudo obtener el email desde Cognito: {}", e.getMessage());
            return null;
        }
    }
}
