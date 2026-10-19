package cl.duoc.dsy1107.rabbitadmin.api;

import cl.duoc.dsy1107.rabbitadmin.dto.ColaInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.CrearColaRequest;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Administración de colas.
 * <pre>
 * POST   /api/admin/queues                    crear cola                 201 / 400 / 404 / 409
 * GET    /api/admin/queues                    listar con métricas        200
 * GET    /api/admin/queues/{nombre}           detalle y métricas         200 / 404
 * DELETE /api/admin/queues/{nombre}           eliminar                   204 / 403 / 404 / 409
 * DELETE /api/admin/queues/{nombre}/mensajes  purgar (limpieza)          200 / 404
 * </pre>
 */
@Tag(name = "Colas", description = "Crear, consultar, purgar y eliminar colas")
@RestController
@RequestMapping("/api/admin/queues")
public class QueueController {

    private final RabbitAdminService adminService;

    public QueueController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @Operation(summary = "Crea una cola con TTL, largo máximo y dead letter opcionales")
    @PostMapping
    public ResponseEntity<ColaInfo> crear(@Valid @RequestBody CrearColaRequest request) {
        ColaInfo cola = adminService.crearCola(request);
        return ResponseEntity.created(URI.create("/api/admin/queues/" + cola.nombre())).body(cola);
    }

    @Operation(summary = "Lista las colas con mensajes listos, sin confirmar y consumidores")
    @GetMapping
    public List<ColaInfo> listar() {
        return adminService.listarColas();
    }

    @Operation(summary = "Detalle y métricas de una cola")
    @GetMapping("/{nombre}")
    public ColaInfo obtener(
            @PathVariable @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
            String nombre) {
        return adminService.obtenerCola(nombre);
    }

    @Operation(summary = "Elimina una cola; puede exigir que esté vacía y sin consumidores")
    @DeleteMapping("/{nombre}")
    public ResponseEntity<Void> eliminar(
            @PathVariable @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
            String nombre,
            @RequestParam(defaultValue = "false") boolean soloSiVacia,
            @RequestParam(defaultValue = "false") boolean soloSiSinConsumidores) {
        adminService.eliminarCola(nombre, soloSiVacia, soloSiSinConsumidores);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Limpia la cola: elimina todos sus mensajes sin borrarla")
    @DeleteMapping("/{nombre}/mensajes")
    public Map<String, Object> purgar(
            @PathVariable @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
            String nombre) {
        int eliminados = adminService.purgarCola(nombre);
        return Map.of("cola", nombre, "mensajesEliminados", eliminados);
    }
}
