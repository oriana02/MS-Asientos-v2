package com.ticketfilms.msasientos;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// Levanta todo el contexto: necesita base de datos, Kafka y COGNITO_ISSUER_URI.
@SpringBootTest
@Disabled("se prueba manualmente: requiere BD, Kafka y variables de entorno")
class MsasientosApplicationTests {

    @Test
    void contextLoads() {
    }

}
