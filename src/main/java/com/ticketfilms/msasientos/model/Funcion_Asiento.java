package com.ticketfilms.msasientos.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Table(name= "funcion_asiento")
@Data
public class Funcion_Asiento {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name="funcion_id", nullable = false)
    private Long funcion_id;

    @Column(name= "asiento_id", nullable = false)
    private Long asiento_id;

    @Column(name= "estado", nullable = false)
    private String estado;

    @Column(name= "usuario_id")
    private String usuario_id;

    // Correo de quien reservó: se usa solo para avisarle si la reserva vence
    @Column(name= "email_usuario")
    private String email_usuario;

    @Column(name= "reservado_hasta")
    private LocalDateTime reservado_hasta;

    @Column(name= "actualizado_en", nullable = false)
    private LocalDateTime actualizado_en;
}
