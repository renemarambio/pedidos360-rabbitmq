package cl.duoc.dsy1107.rabbitadmin.api;

import cl.duoc.dsy1107.rabbitadmin.dto.NodoInfo;
import cl.duoc.dsy1107.rabbitadmin.service.RabbitAdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Estado del clúster RabbitMQ: GET /api/admin/cluster/nodos. */
@Tag(name = "Clúster", description = "Nodos del clúster RabbitMQ")
@RestController
@RequestMapping("/api/admin/cluster")
public class ClusterController {

    private final RabbitAdminService adminService;

    public ClusterController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @Operation(summary = "Lista los nodos del clúster y si están activos")
    @GetMapping("/nodos")
    public List<NodoInfo> nodos() {
        return adminService.listarNodos();
    }
}
