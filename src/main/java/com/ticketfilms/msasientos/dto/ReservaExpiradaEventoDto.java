package com.ticketfilms.msasientos.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// Mismo formato que lee ms-notificaciones en reserva.expirada.
// asientos va como etiquetas legibles ("F7"), no como ids.
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReservaExpiradaEventoDto {
    private Long funcionId;
    private List<String> asientos;
    private String emailUsuario;
}
