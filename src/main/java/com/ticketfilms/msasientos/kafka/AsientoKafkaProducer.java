package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.ReservaExpiradaEventoDto;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.repository.AsientoRepository;
import java.util.Comparator;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

// Publica reserva.expirada. Si Kafka falla, solo se registra en el log:
// un aviso perdido no debe tumbar la reserva ni el mapa de asientos.
@Slf4j
@Component
@RequiredArgsConstructor
public class AsientoKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AsientoRepository asientoRepository;

    public void enviarReservaExpirada(Long funcionId, List<Long> asientosIds, String emailUsuario) {
        if (emailUsuario == null || emailUsuario.isBlank()) {
            log.warn("Reserva expirada sin email, no hay a quién avisar — funcion={} asientos={}",
                    funcionId, asientosIds);
            return;
        }
        try {
            List<String> etiquetas = asientoRepository.findAllById(asientosIds).stream()
                    .sorted(Comparator.comparing(Asiento::getFila).thenComparing(Asiento::getNumero))
                    .map(a -> a.getFila() + a.getNumero())
                    .toList();
            ReservaExpiradaEventoDto evento = new ReservaExpiradaEventoDto(funcionId, etiquetas, emailUsuario);
            String json = objectMapper.writeValueAsString(evento);
            kafkaTemplate.send(KafkaTopics.RESERVA_EXPIRADA, json).whenComplete((res, ex) -> {
                if (ex != null) {
                    log.error("No se pudo publicar en {} — funcion={} motivo={}",
                            KafkaTopics.RESERVA_EXPIRADA, funcionId, ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Error armando el evento de reserva expirada — funcion={} motivo={}",
                    funcionId, e.getMessage());
        }
    }
}
