package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.ReservaExpiradaEventoDto;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.repository.AsientoRepository;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

// Único lugar de ms-asientos que sabe CÓMO se avisa una reserva vencida.
// El servicio de negocio solo decide QUÉ reservas vencieron y se las entrega.
// Reglas de mensajería que viven aquí:
//  - Un solo aviso por usuario y función, aunque tenga varios asientos vencidos.
//  - Sin email no hay a quién avisar: se omite.
//  - No se avisa a excluirUsuarioId (ej. quien está reservando de nuevo).
//  - Con transacción en curso, el evento sale DESPUÉS del commit; si se revierte,
//    no hay aviso falso.
//  - Si Kafka falla, solo se registra: un aviso perdido no tumba la reserva.
// Hay que llamarlo ANTES de limpiar o reasignar el asiento: los datos del aviso
// (email, ids) se copian en el momento de la llamada.
@Slf4j
@Component
@RequiredArgsConstructor
public class AsientoKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AsientoRepository asientoRepository;

    public void publicarReservasExpiradas(List<Funcion_Asiento> expiradas, String excluirUsuarioId) {
        if (expiradas == null || expiradas.isEmpty()) {
            return;
        }
        Map<String, List<Funcion_Asiento>> porUsuario = expiradas.stream()
                .filter(fa -> fa.getEmail_usuario() != null && !fa.getEmail_usuario().isBlank())
                .filter(fa -> excluirUsuarioId == null || !excluirUsuarioId.equals(fa.getUsuario_id()))
                .collect(Collectors.groupingBy(fa -> fa.getFuncion_id() + "|" + fa.getEmail_usuario()));

        porUsuario.values().forEach(grupo -> {
            Funcion_Asiento primero = grupo.get(0);
            List<Long> ids = grupo.stream().map(Funcion_Asiento::getAsiento_id).toList();
            enviarReservaExpirada(primero.getFuncion_id(), ids, primero.getEmail_usuario());
        });
    }

    private void enviarReservaExpirada(Long funcionId, List<Long> asientosIds, String emailUsuario) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    publicar(funcionId, asientosIds, emailUsuario);
                }
            });
        } else {
            publicar(funcionId, asientosIds, emailUsuario);
        }
    }

    private void publicar(Long funcionId, List<Long> asientosIds, String emailUsuario) {
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
