package cl.duoc.dsy1107.pedidos360;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Microservicio de pedidos de Pedidos360.
 * Recibe pedidos por REST, los publica en RabbitMQ y procesa los eventos
 * de cada dominio (notificaciones, cocina y documentos) de forma asíncrona.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class Pedidos360Application {

    public static void main(String[] args) {
        SpringApplication.run(Pedidos360Application.class, args);
    }
}
