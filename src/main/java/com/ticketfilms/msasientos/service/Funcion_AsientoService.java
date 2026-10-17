package com.ticketfilms.msasientos.service;

import com.ticketfilms.msasientos.client.FuncionClient;
import com.ticketfilms.msasientos.dto.FuncionResponse;
import com.ticketfilms.msasientos.exception.CarteleraNoDisponibleException;
import com.ticketfilms.msasientos.exception.FuncionNoEncontradaException;
import com.ticketfilms.msasientos.kafka.AsientoKafkaProducer;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.model.Sala;
import com.ticketfilms.msasientos.repository.AsientoRepository;
import com.ticketfilms.msasientos.repository.Funcion_AsientoRepository;
import com.ticketfilms.msasientos.repository.SalaRepository;
import feign.FeignException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class Funcion_AsientoService {

    private static final String ESTADO_DISPONIBLE = "DISPONIBLE";
    private static final String ESTADO_RESERVADO = "RESERVADO";
    private static final String ESTADO_OCUPADO = "OCUPADO";
    private static final int MAX_ASIENTOS_POR_RESERVA = 10;

    private final Funcion_AsientoRepository funcion_AsientoRepository;
    private final AsientoRepository asientoRepository;
    private final SalaRepository salaRepository;
    private final FuncionClient funcionClient;
    private final AsientoKafkaProducer asientoKafkaProducer;

    // Solo responde si la reserva venció. No publica nada: avisar es otra responsabilidad.
    private boolean estaExpirado(Funcion_Asiento fa) {
        return ESTADO_RESERVADO.equals(fa.getEstado())
                && fa.getReservado_hasta() != null
                && fa.getReservado_hasta().isBefore(LocalDateTime.now());
    }

    // Publica UN aviso por usuario y función (aunque tenga varios asientos vencidos).
    // Hay que llamarlo ANTES de limpiar o reasignar el asiento, porque lee el email guardado.
    // excluirUsuarioId: no avisa a ese usuario (ej. quien está reservando de nuevo,
    // o quien ya recibe el aviso de compra fallida).
    private void avisarExpiradas(List<Funcion_Asiento> expiradas, String excluirUsuarioId) {
        if (expiradas.isEmpty()) {
            return;
        }
        Map<String, List<Funcion_Asiento>> porUsuario = expiradas.stream()
                .filter(fa -> fa.getEmail_usuario() != null && !fa.getEmail_usuario().isBlank())
                .filter(fa -> excluirUsuarioId == null || !excluirUsuarioId.equals(fa.getUsuario_id()))
                .collect(Collectors.groupingBy(fa -> fa.getFuncion_id() + "|" + fa.getEmail_usuario()));

        porUsuario.values().forEach(grupo -> {
            Funcion_Asiento primero = grupo.get(0);
            List<Long> ids = grupo.stream().map(Funcion_Asiento::getAsiento_id).toList();
            asientoKafkaProducer.enviarReservaExpirada(primero.getFuncion_id(), ids, primero.getEmail_usuario());
        });
    }

    private void dejarDisponible(Funcion_Asiento fa) {
        fa.setEstado(ESTADO_DISPONIBLE);
        fa.setUsuario_id(null);
        fa.setEmail_usuario(null);
        fa.setReservado_hasta(null);
        fa.setActualizado_en(LocalDateTime.now());
    }

    private boolean esReservaPropia(Funcion_Asiento fa, String usuario_id) {
        return ESTADO_RESERVADO.equals(fa.getEstado()) && usuario_id.equals(fa.getUsuario_id());
    }

    private List<Long> ordenarSinRepetidos(List<Long> ids) {
        return ids.stream().distinct().sorted().toList();
    }

    public List<Funcion_Asiento> obtenerMapaFuncion_Asientos(Long funcion_id) {
        List<Funcion_Asiento> asientos = funcion_AsientoRepository.findByFuncion_id(funcion_id);

        if (asientos.isEmpty()) {
            return generarMapaInicial(funcion_id);
        }

        List<Funcion_Asiento> expirados = asientos.stream()
                .filter(this::estaExpirado)
                .collect(Collectors.toList());

        if (!expirados.isEmpty()) {
            avisarExpiradas(expirados, null);
            expirados.forEach(this::dejarDisponible);
            funcion_AsientoRepository.saveAll(expirados);
        }

        return asientos;
    }

    private Sala resolverSala(String salaValor) {
        Sala sala = salaRepository.findBySala_codigo(salaValor);
        if (sala != null) {
            return sala;
        }
        return salaRepository.findByNombre(salaValor)
                .orElseThrow(() -> new IllegalStateException(
                        "No se encontró Sala para el valor '" + salaValor + "' (ni por código ni por nombre)"));
    }

    private List<Funcion_Asiento> generarMapaInicial(Long funcion_id) {
        FuncionResponse funcion;
        try {
            funcion = funcionClient.obtenerFuncionPorId(funcion_id);
        } catch (FeignException.NotFound e) {
            throw new FuncionNoEncontradaException(funcion_id);
        } catch (FeignException e) {
            throw new CarteleraNoDisponibleException(funcion_id, e);
        }

        Sala sala = resolverSala(funcion.getSala());

        // Validación adaptativa: si el recinto es de admisión general, no genera matriz estricta de asientos individuales
        if ("GENERAL".equalsIgnoreCase(sala.getTipoRecinto())) {
            return List.of();
        }

        List<Asiento> asientos = asientoRepository.findBySalaId(sala.getId());

        List<Funcion_Asiento> nuevos = asientos.stream()
                .map(asiento -> {
                    Funcion_Asiento fa = new Funcion_Asiento();
                    fa.setFuncion_id(funcion_id);
                    fa.setAsiento_id(asiento.getId());
                    fa.setEstado(ESTADO_DISPONIBLE);
                    fa.setActualizado_en(LocalDateTime.now());
                    return fa;
                })
                .toList();

        return funcion_AsientoRepository.saveAll(nuevos);
    }

    // Versión sin email (se mantiene para no romper los tests existentes)
    @Transactional
    public boolean reservarAsientos(String usuario_id, Long funcion_id, List<Long> asientosIds) {
        return reservarAsientos(usuario_id, null, funcion_id, asientosIds);
    }

    @Transactional
    public boolean reservarAsientos(String usuario_id, String email_usuario, Long funcion_id, List<Long> asientosIds) {
        if (asientosIds == null || asientosIds.isEmpty()) {
            return false;
        }

        if (ordenarSinRepetidos(asientosIds).size() > MAX_ASIENTOS_POR_RESERVA) {
            return false;
        }

        LocalDateTime expiracion = LocalDateTime.now().plusMinutes(5);
        List<Funcion_Asiento> aReservar = new ArrayList<>();
        List<Funcion_Asiento> vencidasDeOtros = new ArrayList<>();

        for (Long asiento_id : ordenarSinRepetidos(asientosIds)) {
            Optional<Funcion_Asiento> optFuncionAsiento
                    = funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(funcion_id, asiento_id);

            if (optFuncionAsiento.isEmpty()) {
                return false;
            }

            Funcion_Asiento funcion_Asiento = optFuncionAsiento.get();
            boolean expirado = estaExpirado(funcion_Asiento);
            boolean libre = ESTADO_DISPONIBLE.equals(funcion_Asiento.getEstado()) || expirado;

            if (!libre && !esReservaPropia(funcion_Asiento, usuario_id)) {
                return false;
            }

            if (expirado) {
                vencidasDeOtros.add(funcion_Asiento);
            }
            aReservar.add(funcion_Asiento);
        }

        // Todo validado: ahora sí se avisa a los dueños anteriores (antes de pisar sus datos)
        avisarExpiradas(vencidasDeOtros, usuario_id);

        for (Funcion_Asiento funcion_Asiento : aReservar) {
            funcion_Asiento.setEstado(ESTADO_RESERVADO);
            funcion_Asiento.setUsuario_id(usuario_id);
            funcion_Asiento.setEmail_usuario(email_usuario);
            funcion_Asiento.setReservado_hasta(expiracion);
            funcion_Asiento.setActualizado_en(LocalDateTime.now());
        }
        funcion_AsientoRepository.saveAll(aReservar);

        return true;
    }

    @Transactional
    public void marcarAsientosComoOcupados(Long funcionId, List<Long> asientosIds) {
        if (asientosIds == null || asientosIds.isEmpty()) {
            return;
        }

        List<Funcion_Asiento> asientos = new ArrayList<>();
        for (Long asientoId : asientosIds) {
            funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(funcionId, asientoId)
                    .ifPresent(fa -> {
                        fa.setEstado(ESTADO_OCUPADO);
                        fa.setActualizado_en(LocalDateTime.now());
                        asientos.add(fa);
                    });
        }
        funcion_AsientoRepository.saveAll(asientos);
    }

    @Transactional
    public void liberarAsientosPorFalla(Long funcionId, List<Long> asientosIds, String usuarioId) {
        if (asientosIds == null || asientosIds.isEmpty()) {
            return;
        }
        if (usuarioId == null || usuarioId.isBlank()) {
            return;
        }

        List<Funcion_Asiento> aLiberar = new ArrayList<>();

        for (Long asientoId : asientosIds) {
            Optional<Funcion_Asiento> optFuncionAsiento
                    = funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(funcionId, asientoId);

            if (optFuncionAsiento.isEmpty()) {
                // El asiento no existe para esa función: lo ignoramos (no es un error fatal).
                continue;
            }

            Funcion_Asiento fa = optFuncionAsiento.get();

            // Idempotencia: si ya está DISPONIBLE, no hacemos nada (mensaje duplicado).
            if (ESTADO_DISPONIBLE.equals(fa.getEstado())) {
                continue;
            }

            // Solo liberamos si está RESERVADO por el mismo usuario del evento.
            if (ESTADO_RESERVADO.equals(fa.getEstado())
                    && usuarioId.equals(fa.getUsuario_id())) {
                aLiberar.add(fa);
            }
            // Si está OCUPADO o RESERVADO por otro usuario, no tocamos nada.
        }

        if (aLiberar.isEmpty()) {
            return;
        }

        aLiberar.forEach(this::dejarDisponible);
        funcion_AsientoRepository.saveAll(aLiberar);
    }

    @Transactional
    public String confirmarAsientos(String usuario_id, Long funcion_id, List<Long> asientosIds) {
        List<Funcion_Asiento> aConfirmar = new ArrayList<>();

        for (Long asiento_id : ordenarSinRepetidos(asientosIds)) {
            Optional<Funcion_Asiento> optFuncionAsiento
                    = funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(funcion_id, asiento_id);

            if (optFuncionAsiento.isEmpty()) {
                return "ASIENTO_INEXISTENTE";
            }

            Funcion_Asiento funcion_Asiento = optFuncionAsiento.get();

            if (estaExpirado(funcion_Asiento)) {
                // Quien compra ya recibe el aviso de compra.fallida; solo se avisa si el dueño es otro
                avisarExpiradas(List.of(funcion_Asiento), usuario_id);
                dejarDisponible(funcion_Asiento);
                funcion_AsientoRepository.save(funcion_Asiento);
                return "RESERVA_EXPIRADA";
            }

            if (!ESTADO_RESERVADO.equals(funcion_Asiento.getEstado())) {
                return "ASIENTO_NO_RESERVADO";
            }

            if (!usuario_id.equals(funcion_Asiento.getUsuario_id())) {
                return "RESERVA_DE_OTRO_USUARIO";
            }

            aConfirmar.add(funcion_Asiento);
        }

        for (Funcion_Asiento funcion_Asiento : aConfirmar) {
            funcion_Asiento.setEstado(ESTADO_OCUPADO);
            funcion_Asiento.setActualizado_en(LocalDateTime.now());
        }
        funcion_AsientoRepository.saveAll(aConfirmar);

        return "OK";
    }

    @Transactional
    public String liberarAsientos(String usuario_id, Long funcion_id, List<Long> asientosIds) {
        if (asientosIds == null || asientosIds.isEmpty()) {
            return "SOLICITUD_INVALIDA";
        }

        List<Funcion_Asiento> aLiberar = new ArrayList<>();
        List<Funcion_Asiento> vencidasDeOtros = new ArrayList<>();

        for (Long asiento_id : ordenarSinRepetidos(asientosIds)) {
            Optional<Funcion_Asiento> optFuncionAsiento
                    = funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(funcion_id, asiento_id);

            if (optFuncionAsiento.isEmpty()) {
                return "ASIENTO_INEXISTENTE";
            }

            Funcion_Asiento funcion_Asiento = optFuncionAsiento.get();

            if (!ESTADO_RESERVADO.equals(funcion_Asiento.getEstado())) {
                return "ASIENTO_NO_RESERVADO";
            }

            boolean esDelUsuario = usuario_id.equals(funcion_Asiento.getUsuario_id());
            boolean expirado = estaExpirado(funcion_Asiento);

            if (!expirado && !esDelUsuario) {
                return "RESERVA_DE_OTRO_USUARIO";
            }

            if (expirado && !esDelUsuario) {
                vencidasDeOtros.add(funcion_Asiento);
            }
            aLiberar.add(funcion_Asiento);
        }

        avisarExpiradas(vencidasDeOtros, null);

        aLiberar.forEach(this::dejarDisponible);
        funcion_AsientoRepository.saveAll(aLiberar);

        // Liberar a mano NO es una expiración: no se publica reserva.expirada aquí.
        return "OK";
    }
}
