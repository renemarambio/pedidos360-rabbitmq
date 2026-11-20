package cl.duoc.dsy1107.rabbitadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Microservicio administrador de RabbitMQ para Pedidos360.
 * Expone una API REST para crear y eliminar colas, exchanges, bindings y políticas.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class RabbitAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitAdminApplication.class, args);
    }
}
