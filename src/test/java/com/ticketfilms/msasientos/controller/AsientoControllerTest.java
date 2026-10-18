package com.ticketfilms.msasientos.controller;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.client.UsuarioInfoClient;
import com.ticketfilms.msasientos.config.SecurityConfig;
import com.ticketfilms.msasientos.dto.ReservaRequestDto;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.model.Sala;
import com.ticketfilms.msasientos.service.AsientoService;
import com.ticketfilms.msasientos.service.Funcion_AsientoService;
import com.ticketfilms.msasientos.service.SalaService;

// issuer-uri falso: el decoder del JWT es perezoso y no consulta a Cognito al arrancar.
// Los JWT se simulan con jwt() de spring-security-test.
@WebMvcTest(value = AsientoController.class,
        properties = "spring.security.oauth2.resourceserver.jwt.issuer-uri=http://localhost/issuer-de-prueba")
@Import(SecurityConfig.class)
public class AsientoControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private AsientoService asientoService;

    @MockBean
    private Funcion_AsientoService funcion_AsientoService;

    @MockBean
    private SalaService salaService;

    @MockBean
    private UsuarioInfoClient usuarioInfoClient;

    private RequestPostProcessor usuario() {
        return jwt().jwt(j -> j.subject("user123").claim("email", "a@b.cl"));
    }

    private String cuerpoReserva() throws Exception {
        ReservaRequestDto request = new ReservaRequestDto();
        request.setFuncionId(1L);
        request.setAsientosSolicitados(List.of(1L, 2L));
        return objectMapper.writeValueAsString(request);
    }

    // ---------- públicos / protegidos ----------
    @Test
    public void testListarSalaEndpoint_esPublico() throws Exception {
        Sala sala = new Sala();
        sala.setId(1L);
        sala.setNombre("Sala VIP");

        when(salaService.listarSalas()).thenReturn(List.of(sala));

        mockMvc.perform(get("/api/asientos/sala"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testObtenerAsientosPorSalaEndpoint() throws Exception {
        Asiento asiento = new Asiento();
        asiento.setId(1L);
        asiento.setSala_id(1L);

        when(asientoService.obtenerAsientosPorSala(1L)).thenReturn(List.of(asiento));

        mockMvc.perform(get("/api/asientos/sala/1").with(usuario()))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testObtenerMapaFuncionEndpoint() throws Exception {
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setId(1L);
        fa.setFuncion_id(1L);

        when(funcion_AsientoService.obtenerMapaFuncion_Asientos(1L)).thenReturn(List.of(fa));

        mockMvc.perform(get("/api/asientos/mapa/1").with(usuario()))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testMapaSinToken_devuelve401() throws Exception {
        mockMvc.perform(get("/api/asientos/mapa/1"))
                .andExpect(status().isUnauthorized());
    }

    // ---------- reservar ----------
    @Test
    public void testReservaAsientosEndpoint() throws Exception {
        when(funcion_AsientoService.reservarAsientos(anyString(), anyString(), anyLong(), anyList()))
                .thenReturn(true);

        mockMvc.perform(post("/api/asientos/reserva")
                .with(usuario())
                .contentType(MediaType.APPLICATION_JSON)
                .content(cuerpoReserva()))
                .andExpect(status().isOk());
    }

    @Test
    public void testReservaAsientosNoDisponibles_devuelve400() throws Exception {
        when(funcion_AsientoService.reservarAsientos(anyString(), anyString(), anyLong(), anyList()))
                .thenReturn(false);

        mockMvc.perform(post("/api/asientos/reserva")
                .with(usuario())
                .contentType(MediaType.APPLICATION_JSON)
                .content(cuerpoReserva()))
                .andExpect(status().isBadRequest());
    }

    // ---------- liberar ----------
    @Test
    public void testLiberarReservaEndpoint() throws Exception {
        when(funcion_AsientoService.liberarAsientos(anyString(), anyLong(), anyList()))
                .thenReturn("OK");

        mockMvc.perform(delete("/api/asientos/reserva")
                .with(usuario())
                .contentType(MediaType.APPLICATION_JSON)
                .content(cuerpoReserva()))
                .andExpect(status().isNoContent());
    }

    // ---------- confirmar ----------
    @Test
    public void testConfirmarAsientosEndpoint() throws Exception {
        when(funcion_AsientoService.confirmarAsientos(anyString(), anyLong(), anyList()))
                .thenReturn("OK");

        mockMvc.perform(put("/api/asientos/confirmar")
                .with(usuario())
                .contentType(MediaType.APPLICATION_JSON)
                .content(cuerpoReserva()))
                .andExpect(status().isOk());
    }

    @Test
    public void testConfirmarReservaExpirada_devuelve410() throws Exception {
        when(funcion_AsientoService.confirmarAsientos(anyString(), anyLong(), anyList()))
                .thenReturn("RESERVA_EXPIRADA");

        mockMvc.perform(put("/api/asientos/confirmar")
                .with(usuario())
                .contentType(MediaType.APPLICATION_JSON)
                .content(cuerpoReserva()))
                .andExpect(status().isGone());
    }
}
