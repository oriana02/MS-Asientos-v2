package com.ticketfilms.msasientos.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ticketfilms.msasientos.dto.ReservaRequestDto;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.model.Sala;
import com.ticketfilms.msasientos.service.AsientoService;
import com.ticketfilms.msasientos.service.Funcion_AsientoService;
import com.ticketfilms.msasientos.service.SalaService;
import com.ticketfilms.msasientos.client.UsuarioInfoClient;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/asientos")
@RequiredArgsConstructor
public class AsientoController {

    private final AsientoService asientoService;
    private final SalaService salaService;
    private final Funcion_AsientoService funcion_AsientoService;
    private final UsuarioInfoClient usuarioInfoClient;

    @GetMapping("/sala")
    public ResponseEntity<List<Sala>> listarSalas() {
        return ResponseEntity.ok(salaService.listarSalas());
    }

    @GetMapping("/sala/{salaId}")
    public ResponseEntity<List<Asiento>> obtenerAsientosPorSala(@PathVariable Long salaId) {
        return ResponseEntity.ok(asientoService.obtenerAsientosPorSala(salaId));
    }

    @GetMapping("/mapa/{funcionId}")
    public ResponseEntity<List<Funcion_Asiento>> obtenerMapaFuncion(@PathVariable Long funcionId) {
        List<Funcion_Asiento> mapa = funcion_AsientoService.obtenerMapaFuncion_Asientos(funcionId);
        return ResponseEntity.ok(mapa);
    }

    @PostMapping("/reserva")
    public ResponseEntity<String> reservaAsientos(
            @RequestBody ReservaRequestDto request,
            @AuthenticationPrincipal Jwt jwt) {
        String usuarioId = jwt.getSubject();

        // El access token de Cognito normalmente no trae email: si falta, se consulta a Cognito
        String email = jwt.getClaimAsString("email");
        if (email == null) {
            email = usuarioInfoClient.obtenerEmail(jwt.getTokenValue());
        }

        boolean exito = funcion_AsientoService.reservarAsientos(
                usuarioId,
                email,
                request.getFuncionId(),
                request.getAsientosSolicitados()
        );
        if (exito) {
            return ResponseEntity.ok("Asientos reservados temporalmente con éxito");
        } else {
            return ResponseEntity.badRequest().body("No pudo completarse esta reserva. Algunos asientos ya no se encuentran disponibles");
        }
    }

    @DeleteMapping("/reserva")
    public ResponseEntity<String> liberarReserva(
            @RequestBody ReservaRequestDto request,
            @AuthenticationPrincipal Jwt jwt) {
        String usuarioId = jwt.getSubject();

        String resultado = funcion_AsientoService.liberarAsientos(
                usuarioId,
                request.getFuncionId(),
                request.getAsientosSolicitados()
        );

        return switch (resultado) {
            case "OK" ->
                ResponseEntity.noContent().build();
            case "SOLICITUD_INVALIDA" ->
                ResponseEntity.badRequest().body("Debes indicar la función y al menos un asiento");
            case "ASIENTO_INEXISTENTE" ->
                ResponseEntity.status(404).body("Uno o más asientos no existen para esta función");
            case "ASIENTO_NO_RESERVADO" ->
                ResponseEntity.status(409).body("Uno o más asientos no están reservados");
            case "RESERVA_DE_OTRO_USUARIO" ->
                ResponseEntity.status(403).body("La reserva pertenece a otro usuario");
            default ->
                ResponseEntity.internalServerError().body("Error desconocido");
        };
    }

    @PutMapping("/confirmar")
    public ResponseEntity<String> confirmarAsientos(
            @RequestBody ReservaRequestDto request,
            @AuthenticationPrincipal Jwt jwt) {
        String usuarioId = jwt.getSubject();

        String resultado = funcion_AsientoService.confirmarAsientos(
                usuarioId,
                request.getFuncionId(),
                request.getAsientosSolicitados()
        );

        return switch (resultado) {
            case "OK" ->
                ResponseEntity.ok("Asientos confirmados con éxito");
            case "ASIENTO_INEXISTENTE" ->
                ResponseEntity.badRequest().body("Uno o más asientos no existen para esta función");
            case "ASIENTO_NO_RESERVADO" ->
                ResponseEntity.badRequest().body("Uno o más asientos no están reservados");
            case "RESERVA_DE_OTRO_USUARIO" ->
                ResponseEntity.status(403).body("La reserva pertenece a otro usuario");
            case "RESERVA_EXPIRADA" ->
                ResponseEntity.status(410).body("La reserva expiró, vuelve a seleccionar tus asientos");
            default ->
                ResponseEntity.internalServerError().body("Error desconocido");
        };
    }
}
