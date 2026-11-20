package cl.duoc.dsy1107.rabbitadmin.api;

import cl.duoc.dsy1107.rabbitadmin.dto.BindingInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.BindingRequest;
import cl.duoc.dsy1107.rabbitadmin.service.RabbitAdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Administración de bindings (exchange -> cola).
 * <pre>
 * POST   /api/admin/bindings   crear     201 / 400 / 404 / 409
 * GET    /api/admin/bindings   listar    200
 * DELETE /api/admin/bindings   eliminar  204 / 400 / 404   (mismo cuerpo que el POST)
 * </pre>
 */
@Tag(name = "Bindings", description = "Enlazar y desenlazar colas con exchanges")
@RestController
@RequestMapping("/api/admin/bindings")
public class BindingController {

    private final RabbitAdminService adminService;

    public BindingController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @Operation(summary = "Enlaza una cola a un exchange con una routing key")
    @PostMapping
    public ResponseEntity<BindingInfo> crear(@Valid @RequestBody BindingRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(adminService.crearBinding(request));
    }

    @Operation(summary = "Lista los bindings definidos (sin los automáticos del exchange por defecto)")
    @GetMapping
    public List<BindingInfo> listar() {
        return adminService.listarBindings();
    }

    @Operation(summary = "Elimina un binding indicando exchange, cola y routing key")
    @DeleteMapping
    public ResponseEntity<Void> eliminar(@Valid @RequestBody BindingRequest request) {
        adminService.eliminarBinding(request);
        return ResponseEntity.noContent().build();
    }
}
