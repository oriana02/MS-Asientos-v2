package com.ticketfilms.msasientos.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.ticketfilms.msasientos.kafka.AsientoKafkaProducer;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.repository.Funcion_AsientoRepository;

@ExtendWith(MockitoExtension.class)
public class Funcion_AsientosServiceTest {

    @Mock
    private Funcion_AsientoRepository funcion_AsientoRepository;

    // El servicio solo decide qué reservas vencieron; publicar es del productor
    @Mock
    private AsientoKafkaProducer asientoKafkaProducer;

    @InjectMocks
    private Funcion_AsientoService funcion_AsientoService;

    @Test
    public void testObtenerMapasFuncionAsientos() {
        Long funcionId = 1L;
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setId(1L);
        fa.setFuncion_id(funcionId);
        fa.setEstado("DISPONIBLE");

        when(funcion_AsientoRepository.findByFuncion_id(funcionId)).thenReturn(List.of(fa));

        List<Funcion_Asiento> mapa = funcion_AsientoService.obtenerMapaFuncion_Asientos(funcionId);
        assertNotNull(mapa);
        assertEquals(1, mapa.size());
        verify(funcion_AsientoRepository, times(1)).findByFuncion_id(funcionId);
    }

    @Test
    public void testReservarAsientosExitoso() {
        Long funcionId = 1L;
        Long asientoId = 10L;
        String usuarioId = "user123";

        Funcion_Asiento funcionAsiento = new Funcion_Asiento();
        funcionAsiento.setId(1L);
        funcionAsiento.setFuncion_id(funcionId);
        funcionAsiento.setAsiento_id(asientoId);
        funcionAsiento.setEstado("DISPONIBLE");

        when(funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(funcionId, asientoId))
                .thenReturn(Optional.of(funcionAsiento));

        when(funcion_AsientoRepository.saveAll(anyList())).thenReturn(List.of(funcionAsiento));

        boolean resultado = funcion_AsientoService.reservarAsientos(usuarioId, funcionId, List.of(asientoId));

        assertTrue(resultado);
        assertEquals("RESERVADO", funcionAsiento.getEstado());
        assertEquals(usuarioId, funcionAsiento.getUsuario_id());
    }
}
