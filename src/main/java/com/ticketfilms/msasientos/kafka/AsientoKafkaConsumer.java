package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import com.ticketfilms.msasientos.service.Funcion_AsientoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Responsabilidad de este consumidor: cuando una compra falla en ms-boletos,
// liberar los asientos reservados.
//
// IMPORTANTE: ya NO escucha boleto.comprado. La confirmación de asientos
// sigue siendo la llamada síncrona PUT /api/asientos/confirmar; ese evento
// lo consume solo ms-notificaciones.
//
// IMPORTANTE 2: ya NO hay try/catch que se traga el error. Si algo falla, la
// excepción sube y KafkaErrorHandlingConfig decide: reintentar o mandar al DLT.
@Slf4j
@Component
@RequiredArgsConstructor
public class AsientoKafkaConsumer {

    private final Funcion_AsientoService funcion_AsientoService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.COMPRA_FALLIDA)
    public void consumirCompraFallida(String mensaje) {
        AsientoEventoDto evento = leerEvento(mensaje);
        log.info("Evento recibido [{}] funcion={} usuario={}",
                KafkaTopics.COMPRA_FALLIDA, evento.getFuncionId(), evento.getUsuarioId());

        // El servicio debe liberar SOLO los asientos que estén RESERVADOS por ese
        // usuario (evento.getUsuarioId()). Así, si la compra falló porque la reserva
        // era de otra persona, no se le quitan los asientos al dueño real.
        // También debe aguantar que el mismo mensaje llegue dos veces (si ya están
        // libres, no hace nada y no lanza error).
        funcion_AsientoService.liberarAsientosPorFalla(
                evento.getFuncionId(), evento.getAsientosIds(), evento.getUsuarioId());
    }

    // Si el JSON está mal o le faltan datos, es un error definitivo: va directo al DLT
    private AsientoEventoDto leerEvento(String mensaje) {
        AsientoEventoDto evento;
        try {
            evento = objectMapper.readValue(mensaje, AsientoEventoDto.class);
        } catch (JsonProcessingException e) {
            throw new MensajeInvalidoException("El mensaje no es un JSON válido", e);
        }
        if (evento.getFuncionId() == null
                || evento.getAsientosIds() == null
                || evento.getAsientosIds().isEmpty()
                || evento.getUsuarioId() == null) {
            throw new MensajeInvalidoException("Al mensaje le faltan datos obligatorios (funcionId, asientosIds, usuarioId)");
        }
        return evento;
    }
}

