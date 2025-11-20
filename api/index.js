const oracledb = require("oracledb");

// =============================
// CONFIG DB
// =============================
const dbConfigMexico = {
  user: process.env.DB_MEXICO_USER || "ADMIN",
  password: process.env.DB_MEXICO_PASSWORD || "Adi4APH_827HK",
  connectString: "(description=(retry_count=20)(retry_delay=3)" +
    "(address=(protocol=tcps)(port=1522)(host=adb.mx-queretaro-1.oraclecloud.com))" +
    "(connect_data=(service_name=g1eba54685c8450_dbmexico_tp.adb.oraclecloud.com))" +
    "(security=(ssl_server_dn_match=no)))"
};

const dbConfigCanada = {
  user: process.env.DB_CANADA_USER || "ADMIN",
  password: process.env.DB_CANADA_PASSWORD || "Adi4APH_827HK",
  connectString: "(description=(retry_count=20)(retry_delay=3)" +
    "(address=(protocol=tcps)(port=1522)(host=adb.mx-queretaro-1.oraclecloud.com))" +
    "(connect_data=(service_name=g1eba54685c8450_dbcanada_tp.adb.oraclecloud.com))" +
    "(security=(ssl_server_dn_match=no)))"
};

let poolMexico = null;
let poolCanada = null;
let cambiosPendientes = []; // Cola de cambios para replicar
let initializingMexico = false; // Evitar inicializaciones múltiples
let initializingCanada = false;

// =============================
// Crear pools (mejorado)
// =============================
async function initPools() {
  // México
  if (!poolMexico && !initializingMexico) {
    initializingMexico = true;
    const mexicoAntes = false;
    
    try {
      console.log("🔄 Intentando conectar a México...");
      poolMexico = await oracledb.createPool({
        ...dbConfigMexico,
        poolMin: 0,
        poolMax: 3,
        poolIncrement: 1,
        poolTimeout: 30,
        queueTimeout: 10000
      });
      console.log("✅ Pool México iniciado");
      
      if (!mexicoAntes && cambiosPendientes.length > 0) {
        console.log("🔄 México volvió online, replicando cambios pendientes...");
        setTimeout(() => replicarCambiosPendientes("MEXICO"), 1000);
      }
    } catch (err) {
      console.log("❌ Error pool México:", err.message);
      poolMexico = null;
    } finally {
      initializingMexico = false;
    }
  }

  // Canadá
  if (!poolCanada && !initializingCanada) {
    initializingCanada = true;
    const canadaAntes = false;
    
    try {
      console.log("🔄 Intentando conectar a Canadá...");
      poolCanada = await oracledb.createPool({
        ...dbConfigCanada,
        poolMin: 0,
        poolMax: 3,
        poolIncrement: 1,
        poolTimeout: 30,
        queueTimeout: 10000
      });
      console.log("✅ Pool Canadá iniciado");
      
      if (!canadaAntes && cambiosPendientes.length > 0) {
        console.log("🔄 Canadá volvió online, replicando cambios pendientes...");
        setTimeout(() => replicarCambiosPendientes("CANADA"), 1000);
      }
    } catch (err) {
      console.log("❌ Error pool Canadá:", err.message);
      poolCanada = null;
    } finally {
      initializingCanada = false;
    }
  }
}

// =============================
// Replicar cambios pendientes
// =============================
async function replicarCambiosPendientes(region) {
  const pool = region === "MEXICO" ? poolMexico : poolCanada;
  if (!pool) return;

  const cambiosReplicados = [];
  
  for (const cambio of cambiosPendientes) {
    if (cambio.destino !== region) continue;
    
    try {
      const conn = await pool.getConnection();
      await conn.execute(cambio.query, cambio.binds, { autoCommit: true });
      await conn.close();
      console.log(`✅ Replicado en ${region}: ${cambio.tipo}`);
      cambiosReplicados.push(cambio);
    } catch (err) {
      console.log(`❌ Error replicando en ${region}:`, err.message);
    }
  }
  
  // Eliminar cambios ya replicados
  cambiosPendientes = cambiosPendientes.filter(c => !cambiosReplicados.includes(c));
  console.log(`📊 Cambios pendientes restantes: ${cambiosPendientes.length}`);
}

// =============================
// Obtener conexión con reintentos (optimizado)
// =============================
async function getConnection(retries = 2) {
  await initPools();

  for (let i = 0; i < retries; i++) {
    // Intentar México
    if (poolMexico) {
      try {
        const conn = await poolMexico.getConnection();
        return { conn, region: "MEXICO" };
      } catch (err) {
        console.log(`⚠️ México intento ${i + 1} falló:`, err.message);
        
        // Solo recrear si es error de conexión rota
        if (err.message.includes('NJS-500') || err.message.includes('ECONNRESET')) {
          console.log("🔄 Recreando pool México...");
          try { await poolMexico.close(0); } catch {}
          poolMexico = null;
          initializingMexico = false;
        }
      }
    }

    // Intentar Canadá
    if (poolCanada) {
      try {
        const conn = await poolCanada.getConnection();
        return { conn, region: "CANADA" };
      } catch (err) {
        console.log(`⚠️ Canadá intento ${i + 1} falló:`, err.message);
        
        if (err.message.includes('NJS-500') || err.message.includes('ECONNRESET')) {
          console.log("🔄 Recreando pool Canadá...");
          try { await poolCanada.close(0); } catch {}
          poolCanada = null;
          initializingCanada = false;
        }
      }
    }
    
    // Si ambos fallaron, reintentar pools
    if (!poolMexico && !poolCanada) {
      await new Promise(r => setTimeout(r, 500));
      await initPools();
    }
  }

  throw new Error("❌ Ninguna base de datos disponible");
}

// =============================
// Replicación mejorada
// =============================
async function replicar(query, binds, origen, tipo = "INSERT") {
  const destino = origen === "MEXICO" ? "CANADA" : "MEXICO";
  const poolDestino = destino === "MEXICO" ? poolMexico : poolCanada;

  if (!poolDestino) {
    console.log(`⚠️ ${destino} offline, guardando cambio para replicar después`);
    cambiosPendientes.push({ query, binds, destino, tipo, timestamp: Date.now() });
    return false;
  }

  try {
    const conn = await poolDestino.getConnection();
    await conn.execute(query, binds, { autoCommit: true });
    await conn.close();
    console.log(`🔄 Replicado en ${destino}`);
    return true;
  } catch (err) {
    console.log(`⚠️ Error replicando en ${destino}:`, err.message);
    cambiosPendientes.push({ query, binds, destino, tipo, timestamp: Date.now() });
    return false;
  }
}

// =============================
// Helper parseBody
// =============================
async function parseBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk.toString()));
    req.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch {
        resolve({});
      }
    });
  });
}

// =============================
// API
// =============================
module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();

  let path = req.url.replace("/api", "").split("?")[0];

  try {
    // INICIALIZAR (nuevo)
    if (path === "/init") {
      await initPools();
      return res.json({ 
        success: true,
        mexico: poolMexico ? "INICIADO" : "FALLO",
        canada: poolCanada ? "INICIADO" : "FALLO"
      });
    }

    // STATUS
    if (path === "/status") {
      return res.json({
        mexico: poolMexico ? "CONECTADA" : "DESCONECTADA",
        canada: poolCanada ? "CONECTADA" : "DESCONECTADA",
        cambiosPendientes: cambiosPendientes.length
      });
    }

    // DASHBOARD
    if (path === "/dashboard") {
      const { conn, region } = await getConnection();
      try {
        const stats = await conn.execute(
          `SELECT 
            (SELECT COUNT(*) FROM envios WHERE estatus IN ('PENDIENTE','EN_TRANSITO')) AS ENVIOS_ACTIVOS,
            (SELECT COUNT(*) FROM clientes) AS TOTAL_CLIENTES,
            (SELECT COUNT(*) FROM almacenes) AS TOTAL_ALMACENES,
            (SELECT COUNT(*) FROM viajes) AS VIAJES_ACTIVOS
          FROM dual`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ 
          data: stats.rows[0] || {}, 
          region,
          cambiosPendientes: cambiosPendientes.length
        });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // RASTREAR ENVÍO (VISTA CLIENTE)
    if (path.startsWith("/rastrear/")) {
      const guia = path.replace("/rastrear/", "");
      const { conn, region } = await getConnection();
      
      try {
        const r = await conn.execute(
          `SELECT e.numero_guia, e.estatus, e.fecha_creacion, e.fecha_entrega_estimada,
                  c.nombre || ' ' || c.apellidos AS cliente,
                  e.destinatario_nombre, e.destinatario_direccion,
                  ao.ciudad || ', ' || ao.pais AS origen,
                  ad.ciudad || ', ' || ad.pais AS destino,
                  v.responsable_nombre, u.placa
           FROM envios e
           JOIN clientes c ON e.cliente_id = c.id
           JOIN almacenes ao ON ao.id = e.almacen_origen_id
           JOIN almacenes ad ON ad.id = e.almacen_destino_id
           LEFT JOIN viajes v ON v.id = e.viaje_id
           LEFT JOIN unidades u ON u.id = v.unidad_id
           WHERE e.numero_guia = :guia`,
          { guia },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        
        // Historial
        const hist = await conn.execute(
          `SELECT estatus_nuevo, observaciones, ubicacion, 
                  TO_CHAR(fecha_cambio, 'DD/MM/YYYY HH24:MI') as fecha
           FROM historial_envios 
           WHERE envio_id = (SELECT id FROM envios WHERE numero_guia = :guia)
           ORDER BY fecha_cambio DESC`,
          { guia },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        
        await conn.close();
        return res.json({ 
          envio: r.rows[0] || null, 
          historial: hist.rows,
          region 
        });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // ENVIOS
    if (path === "/envios" && req.method === "GET") {
      const { conn, region } = await getConnection();
      try {
        const r = await conn.execute(
          `SELECT e.id, e.numero_guia,
                  c.nombre || ' ' || c.apellidos AS cliente,
                  ao.ciudad AS origen, ad.ciudad AS destino,
                  e.estatus, e.costo_envio,
                  TO_CHAR(e.fecha_creacion, 'DD/MM/YYYY') as fecha
           FROM envios e
           JOIN clientes c ON e.cliente_id = c.id
           JOIN almacenes ao ON ao.id = e.almacen_origen_id
           JOIN almacenes ad ON ad.id = e.almacen_destino_id
           ORDER BY e.fecha_creacion DESC`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ data: r.rows, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // CREAR ENVÍO
    if (path === "/envios" && req.method === "POST") {
      const body = await parseBody(req);
      const { conn, region } = await getConnection();
      const numeroGuia = "PKG-" + Date.now().toString(36).toUpperCase();

      const query = `
        INSERT INTO envios 
        (numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
         descripcion, peso_kg, costo_envio, destinatario_nombre,
         destinatario_telefono, destinatario_direccion, estatus, 
         fecha_creacion, region_origen, region_destino)
        VALUES (:guia, :cliente, :origen, :destino, :descripcion, :peso, 
                :costo, :dest_nombre, :dest_tel, :dest_dir, 'PENDIENTE', 
                CURRENT_TIMESTAMP, :reg_origen, :reg_destino)
      `;

      const binds = {
        guia: numeroGuia,
        cliente: body.cliente_id,
        origen: body.almacen_origen_id,
        destino: body.almacen_destino_id,
        descripcion: body.descripcion || null,
        peso: body.peso_kg,
        costo: body.costo_envio,
        dest_nombre: body.destinatario_nombre,
        dest_tel: body.destinatario_telefono || null,
        dest_dir: body.destinatario_direccion || null,
        reg_origen: region,
        reg_destino: body.region_destino || region
      };

      try {
        await conn.execute(query, binds, { autoCommit: true });
        
        // Registrar en historial
        await conn.execute(
          `INSERT INTO historial_envios 
           (envio_id, estatus_nuevo, ubicacion, observaciones)
           VALUES ((SELECT id FROM envios WHERE numero_guia = :guia), 
                   'PENDIENTE', 'Sistema', 'Envío creado')`,
          { guia: numeroGuia },
          { autoCommit: true }
        );
        
        await conn.close();
        
        // Replicar
        await replicar(query, binds, region, "INSERT_ENVIO");
        
        return res.json({ success: true, numeroGuia, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // ACTUALIZAR ESTATUS ENVÍO
    if (path.startsWith("/envios/") && req.method === "PUT") {
      const id = path.split("/")[2];
      const body = await parseBody(req);
      const { conn, region } = await getConnection();

      try {
        await conn.execute(
          `UPDATE envios SET estatus = :estatus WHERE id = :id`,
          { estatus: body.estatus, id },
          { autoCommit: true }
        );
        
        await conn.execute(
          `INSERT INTO historial_envios 
           (envio_id, estatus_anterior, estatus_nuevo, ubicacion, observaciones)
           SELECT :id, estatus, :nuevo, :ubicacion, :obs FROM envios WHERE id = :id`,
          { 
            id, 
            nuevo: body.estatus, 
            ubicacion: body.ubicacion || 'Sistema',
            obs: body.observaciones || null
          },
          { autoCommit: true }
        );
        
        await conn.close();
        return res.json({ success: true });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // REPORTES - CLIENTES
    if (path === "/reportes/clientes") {
      const { conn, region } = await getConnection();
      try {
        const r = await conn.execute(
          `SELECT c.nombre || ' ' || c.apellidos AS nombre_completo,
                  c.curp,
                  FLOOR(MONTHS_BETWEEN(SYSDATE, c.fecha_nacimiento)/12) AS edad,
                  TO_CHAR(c.fecha_nacimiento, 'DD/MM/YYYY') as fecha_nacimiento,
                  COUNT(e.id) AS num_envios,
                  NVL(SUM(p.monto), 0) AS total_pagado,
                  NVL(SUM(e.costo_envio), 0) - NVL(SUM(p.monto), 0) AS deuda
           FROM clientes c
           LEFT JOIN envios e ON e.cliente_id = c.id
           LEFT JOIN pagos p ON p.envio_id = e.id
           GROUP BY c.id, c.nombre, c.apellidos, c.curp, c.fecha_nacimiento
           ORDER BY c.id`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ data: r.rows, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // REPORTES - ALMACENES
    if (path === "/reportes/almacenes") {
      const { conn, region } = await getConnection();
      try {
        const r = await conn.execute(
          `SELECT a.nombre, a.direccion, TO_CHAR(SYSDATE, 'DD/MM/YYYY') as fecha_reporte,
                  (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='PENDIENTE') AS paquetes_espera,
                  (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='EN_TRANSITO') AS paquetes_transito,
                  (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='ENTREGADO') AS paquetes_enviados
           FROM almacenes a
           WHERE a.activo = 1
           ORDER BY a.id`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ data: r.rows, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // REPORTES - VIAJES
    if (path === "/reportes/viajes") {
      const { conn, region } = await getConnection();
      try {
        const r = await conn.execute(
          `SELECT v.responsable_nombre, u.placa, 
                  TO_CHAR(v.fecha_salida, 'DD/MM/YYYY') as fecha,
                  (SELECT COUNT(*) FROM envios WHERE viaje_id = v.id) AS num_paquetes
           FROM viajes v
           JOIN unidades u ON u.id = v.unidad_id
           ORDER BY v.fecha_salida DESC`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ data: r.rows, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // CLIENTES
    if (path === "/clientes") {
      const { conn, region } = await getConnection();
      try {
        const r = await conn.execute(
          `SELECT c.id, c.nombre, c.apellidos, c.email, c.telefono, c.ciudad, c.pais,
                  COUNT(e.id) AS total_envios, NVL(SUM(p.monto), 0) AS total_pagado
           FROM clientes c
           LEFT JOIN envios e ON e.cliente_id = c.id
           LEFT JOIN pagos p ON p.envio_id = e.id
           GROUP BY c.id, c.nombre, c.apellidos, c.email, c.telefono, c.ciudad, c.pais
           ORDER BY c.id DESC`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ data: r.rows, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // ALMACENES
    if (path === "/almacenes") {
      const { conn, region } = await getConnection();
      try {
        const r = await conn.execute(
          `SELECT a.id, a.nombre, a.ciudad, a.estado, a.pais, a.region,
                  (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='PENDIENTE') AS pendientes,
                  (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='EN_TRANSITO') AS transito,
                  (SELECT COUNT(*) FROM envios WHERE almacen_destino_id = a.id AND estatus='ENTREGADO') AS entregados
           FROM almacenes a WHERE a.activo = 1 ORDER BY a.id`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ data: r.rows, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // VIAJES
    if (path === "/viajes") {
      const { conn, region } = await getConnection();
      try {
        const r = await conn.execute(
          `SELECT v.id, v.responsable_nombre, u.placa, r.nombre AS ruta, v.estatus,
                  (SELECT COUNT(*) FROM envios WHERE viaje_id = v.id) AS paquetes
           FROM viajes v
           JOIN unidades u ON u.id = v.unidad_id
           JOIN rutas r ON r.id = v.ruta_id
           ORDER BY v.id DESC`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await conn.close();
        return res.json({ data: r.rows, region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    return res.status(404).json({ error: "Ruta no encontrada" });

  } catch (err) {
    console.error("🔥 ERROR:", err);
    return res.status(500).json({ 
      error: err.message,
      details: "Error en el servidor"
    });
  }
};