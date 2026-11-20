package cl.duoc.dsy1107.rabbitadmin.api;

import cl.duoc.dsy1107.rabbitadmin.dto.PoliticaInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.PoliticaRequest;
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
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Políticas de retención y limpieza (TTL y largo máximo aplicados por patrón de nombre).
 * <pre>
 * PUT    /api/admin/policies/{nombre}  crear o reemplazar  200 / 400
 * GET    /api/admin/policies           listar              200
 * DELETE /api/admin/policies/{nombre}  eliminar            204 / 404
 * </pre>
 */
@Tag(name = "Políticas", description = "Retención y limpieza de mensajes (TTL / max-length)")
@RestController
@RequestMapping("/api/admin/policies")
public class PoliticaController {

    private final RabbitAdminService adminService;

    public PoliticaController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @Operation(summary = "Crea o reemplaza una política de TTL / largo máximo")
    @PutMapping("/{nombre}")
    public PoliticaInfo aplicar(
            @PathVariable @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
            String nombre,
            @Valid @RequestBody PoliticaRequest request) {
        return adminService.aplicarPolitica(nombre, request);
    }

    @Operation(summary = "Lista las políticas activas")
    @GetMapping
    public List<PoliticaInfo> listar() {
        return adminService.listarPoliticas();
    }

    @Operation(summary = "Elimina una política")
    @DeleteMapping("/{nombre}")
    public ResponseEntity<Void> eliminar(
            @PathVariable @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
            String nombre) {
        adminService.eliminarPolitica(nombre);
        return ResponseEntity.noContent().build();
    }
}
