package com.ticketfilms.msasientos.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.repository.AsientoRepository;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class AsientoKafkaProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private AsientoRepository asientoRepository;

    private AsientoKafkaProducer producer;

    @BeforeEach
    void setUp() {
        producer = new AsientoKafkaProducer(kafkaTemplate, new ObjectMapper(), asientoRepository);
    }

    private Funcion_Asiento reserva(Long asientoId, String usuarioId, String email) {
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setFuncion_id(5L);
        fa.setAsiento_id(asientoId);
        fa.setUsuario_id(usuarioId);
        fa.setEmail_usuario(email);
        return fa;
    }

    private Asiento asiento(Long id, String fila, int numero) {
        Asiento a = new Asiento();
        a.setId(id);
        a.setFila(fila);
        a.setNumero(numero);
        return a;
    }

    @Test
    void variosAsientosDelMismoUsuario_publicaUnSoloAviso() {
        when(asientoRepository.findAllById(anyList()))
                .thenReturn(List.of(asiento(7L, "F", 7), asiento(8L, "F", 8)));
        when(kafkaTemplate.send(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        producer.publicarReservasExpiradas(
                List.of(reserva(7L, "u1", "a@b.cl"), reserva(8L, "u1", "a@b.cl")), null);

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopics.RESERVA_EXPIRADA), json.capture());
        assertTrue(json.getValue().contains("a@b.cl"));
        assertTrue(json.getValue().contains("F7"));
        assertTrue(json.getValue().contains("F8"));
    }

    @Test
    void usuarioExcluido_noSePublicaNada() {
        producer.publicarReservasExpiradas(List.of(reserva(7L, "u1", "a@b.cl")), "u1");

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void sinEmail_noSePublicaNada() {
        producer.publicarReservasExpiradas(List.of(reserva(7L, "u1", null)), null);

        verifyNoInteractions(kafkaTemplate);
    }
}
