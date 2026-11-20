package cl.duoc.dsy1107.rabbitadmin.api;

import cl.duoc.dsy1107.rabbitadmin.dto.CrearExchangeRequest;
import cl.duoc.dsy1107.rabbitadmin.dto.ExchangeInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.ReglasValidacion;
import cl.duoc.dsy1107.rabbitadmin.service.RabbitAdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

/**
 * Administración de exchanges.
 * <pre>
 * POST   /api/admin/exchanges           crear (DIRECT, TOPIC, FANOUT, HEADERS)   201 / 400 / 403 / 409
 * GET    /api/admin/exchanges           listar                                   200
 * DELETE /api/admin/exchanges/{nombre}  eliminar                                 204 / 403 / 404
 * </pre>
 */
@Tag(name = "Exchanges", description = "Crear, listar y eliminar exchanges")
@RestController
@RequestMapping("/api/admin/exchanges")
public class ExchangeController {

    private final RabbitAdminService adminService;

    public ExchangeController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @Operation(summary = "Crea un exchange")
    @PostMapping
    public ResponseEntity<ExchangeInfo> crear(@Valid @RequestBody CrearExchangeRequest request) {
        ExchangeInfo exchange = adminService.crearExchange(request);
        return ResponseEntity.created(URI.create("/api/admin/exchanges/" + exchange.nombre())).body(exchange);
    }

    @Operation(summary = "Lista los exchanges del broker")
    @GetMapping
    public List<ExchangeInfo> listar() {
        return adminService.listarExchanges();
    }

    @Operation(summary = "Elimina un exchange")
    @DeleteMapping("/{nombre}")
    public ResponseEntity<Void> eliminar(
            @PathVariable @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
            String nombre) {
        adminService.eliminarExchange(nombre);
        return ResponseEntity.noContent().build();
    }
}
