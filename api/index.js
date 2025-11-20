const oracledb = require("oracledb");

// =============================
// CONFIG DB - SIN WALLETS (mTLS deshabilitado)
// =============================
const dbConfigMexico = {
  user: process.env.DB_MEXICO_USER || "ADMIN",
  password: process.env.DB_MEXICO_PASSWORD || "Adi4APH_827HK",
  // Usar el string completo del tnsnames.ora directamente
  connectString: "(description=(retry_count=20)(retry_delay=3)" +
    "(address=(protocol=tcps)(port=1522)(host=adb.mx-queretaro-1.oraclecloud.com))" +
    "(connect_data=(service_name=g1eba54685c8450_dbmexico_tp.adb.oraclecloud.com))" +
    "(security=(ssl_server_dn_match=yes)))"
};

const dbConfigCanada = {
  user: process.env.DB_CANADA_USER || "ADMIN",
  password: process.env.DB_CANADA_PASSWORD || "Adi4APH_827HK",
  connectString: "(description=(retry_count=20)(retry_delay=3)" +
    "(address=(protocol=tcps)(port=1522)(host=adb.mx-queretaro-1.oraclecloud.com))" +
    "(connect_data=(service_name=g1eba54685c8450_dbcanada_tp.adb.oraclecloud.com))" +
    "(security=(ssl_server_dn_match=yes)))"
};

let poolMexico = null;
let poolCanada = null;

// =============================
// Crear pools
// =============================
async function initPools() {
  try {
    if (!poolMexico) {
      console.log("🔄 Intentando conectar a México...");
      poolMexico = await oracledb.createPool(dbConfigMexico);
      console.log("✅ Pool México iniciado");
    }
  } catch (err) {
    console.log("❌ Error pool México:", err.message);
  }

  try {
    if (!poolCanada) {
      console.log("🔄 Intentando conectar a Canadá...");
      poolCanada = await oracledb.createPool(dbConfigCanada);
      console.log("✅ Pool Canadá iniciado");
    }
  } catch (err) {
    console.log("❌ Error pool Canadá:", err.message);
  }
}

// =============================
// Obtener conexión automática con reintentos
// =============================
async function getConnection(retries = 3) {
  await initPools();

  for (let i = 0; i < retries; i++) {
    // Intentar México
    if (poolMexico) {
      try {
        console.log(`🔌 Intentando conexión a México (intento ${i + 1}/${retries})`);
        const conn = await poolMexico.getConnection();
        console.log("✅ Conexión a México exitosa");
        return { conn, region: "MEXICO" };
      } catch (err) {
        console.log(`⚠️ México intento ${i + 1} falló:`, err.message);
        
        // Si el pool está roto, recrearlo
        if (err.message.includes('NJS-500') || err.message.includes('ECONNRESET')) {
          console.log("🔄 Recreando pool México...");
          try {
            await poolMexico.close(0);
          } catch {}
          poolMexico = null;
          await initPools();
        }
        
        if (i < retries - 1) await new Promise(r => setTimeout(r, 1000));
      }
    }

    // Intentar Canadá
    if (poolCanada) {
      try {
        console.log(`🔌 Intentando conexión a Canadá (intento ${i + 1}/${retries})`);
        const conn = await poolCanada.getConnection();
        console.log("✅ Conexión a Canadá exitosa");
        return { conn, region: "CANADA" };
      } catch (err) {
        console.log(`⚠️ Canadá intento ${i + 1} falló:`, err.message);
        
        if (err.message.includes('NJS-500') || err.message.includes('ECONNRESET')) {
          console.log("🔄 Recreando pool Canadá...");
          try {
            await poolCanada.close(0);
          } catch {}
          poolCanada = null;
          await initPools();
        }
        
        if (i < retries - 1) await new Promise(r => setTimeout(r, 1000));
      }
    }
  }

  throw new Error("❌ Ninguna base de datos disponible después de reintentos");
}

// =============================
// Replicación
// =============================
async function replicar(query, binds, origen) {
  const destino = origen === "MEXICO" ? "CANADA" : "MEXICO";
  const poolDestino = destino === "MEXICO" ? poolMexico : poolCanada;

  if (!poolDestino) return;

  try {
    const conn = await poolDestino.getConnection();
    await conn.execute(query, binds, { autoCommit: true });
    await conn.close();
    console.log(`🔄 Replicado en ${destino}`);
  } catch (err) {
    console.log(`⚠️ No replicado en ${destino}: ${err.message}`);
  }
}

// =============================
// Helper para parsear body
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
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();

  let path = req.url.replace("/api", "").split("?")[0];

  try {
    // =======================
    // ✅ STATUS
    // =======================
    if (path === "/status") {
      await initPools();
      return res.json({
        mexico: poolMexico ? "CONECTADA" : "DESCONECTADA",
        canada: poolCanada ? "CONECTADA" : "DESCONECTADA"
      });
    }

    // =======================
    // ✅ DASHBOARD - CORREGIDO
    // =======================
    if (path === "/dashboard") {
      console.log("📊 Petición al dashboard recibida");
      const { conn, region } = await getConnection();
      console.log("🔌 Conexión obtenida desde:", region);

      try {
        console.log("⏳ Ejecutando query dashboard simplificada...");
        
        // Query más simple y rápida
        const stats = await Promise.race([
          conn.execute(
            `SELECT 
              COUNT(CASE WHEN estatus IN ('PENDIENTE','EN_TRANSITO') THEN 1 END) AS ENVIOS_ACTIVOS,
              (SELECT COUNT(*) FROM clientes) AS TOTAL_CLIENTES,
              (SELECT COUNT(*) FROM almacenes) AS TOTAL_ALMACENES,
              (SELECT COUNT(*) FROM viajes) AS VIAJES_ACTIVOS
            FROM envios`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
          ),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Query timeout (10s)')), 10000)
          )
        ]);

        console.log("✅ Query completada:", stats.rows[0]);
        await conn.close();

        const data = stats.rows && stats.rows.length > 0 ? stats.rows[0] : {
          ENVIOS_ACTIVOS: 0,
          TOTAL_CLIENTES: 0,
          TOTAL_ALMACENES: 0,
          VIAJES_ACTIVOS: 0
        };

        return res.json({ data, region });
      } catch (err) {
        console.error("❌ Error en dashboard:", err.message);
        await conn.close();
        throw err;
      }
    }

    // =======================
    // ✅ GET /envios
    // =======================
    if (path === "/envios" && req.method === "GET") {
      console.log("📦 Petición a /envios");
      const { conn, region } = await getConnection();

      try {
        const r = await Promise.race([
          conn.execute(
            `SELECT e.id,
                    e.numero_guia,
                    c.nombre || ' ' || c.apellidos AS cliente,
                    ao.ciudad AS origen,
                    ad.ciudad AS destino,
                    e.estatus,
                    e.costo_envio
             FROM envios e
             JOIN clientes c ON e.cliente_id = c.id
             JOIN almacenes ao ON ao.id = e.almacen_origen_id
             JOIN almacenes ad ON ad.id = e.almacen_destino_id
             ORDER BY e.fecha_creacion DESC`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
          ),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Query timeout')), 10000)
          )
        ]);

        console.log("✅ Envíos obtenidos:", r.rows.length);
        await conn.close();
        return res.json({ data: r.rows || [], region });
      } catch (err) {
        console.error("❌ Error en /envios:", err.message);
        await conn.close();
        throw err;
      }
    }

    // =======================
    // ✅ POST /envios
    // =======================
    if (path === "/envios" && req.method === "POST") {
      const body = await parseBody(req);
      const { conn, region } = await getConnection();

      const numeroGuia = "MX-" + Date.now().toString(36).toUpperCase();

      const query = `
        INSERT INTO envios 
        (numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
         descripcion, peso_kg, costo_envio, destinatario_nombre,
         destinatario_telefono, estatus, fecha_creacion)
        VALUES (:guia, :cliente, :origen, :destino,
                :descripcion, :peso, :costo, :destinatario, :telefono, 'PENDIENTE', CURRENT_TIMESTAMP)
      `;

      const binds = {
        guia: numeroGuia,
        cliente: body.cliente_id,
        origen: body.almacen_origen_id,
        destino: body.almacen_destino_id,
        descripcion: body.descripcion || null,
        peso: body.peso_kg,
        costo: body.costo_envio,
        destinatario: body.destinatario_nombre,
        telefono: body.destinatario_telefono || null
      };

      try {
        await conn.execute(query, binds, { autoCommit: true });
        await conn.close();

        // Replicación
        replicar(query, binds, region);

        return res.json({ success: true, numeroGuia });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // =======================
    // ✅ CLIENTES
    // =======================
    if (path === "/clientes") {
      const { conn, region } = await getConnection();

      try {
        const r = await conn.execute(
          `SELECT c.id, c.nombre, c.apellidos, c.email, c.telefono,
                  c.ciudad, c.pais, c.region,
                  COUNT(e.id) AS total_envios,
                  NVL(SUM(p.monto), 0) AS total_pagado
           FROM clientes c
           LEFT JOIN envios e ON e.cliente_id = c.id
           LEFT JOIN pagos p ON p.envio_id = e.id
           GROUP BY c.id, c.nombre, c.apellidos, c.email, c.telefono,
                    c.ciudad, c.pais, c.region
           ORDER BY c.id DESC`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        await conn.close();
        return res.json({ data: r.rows || [], region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // =======================
    // ✅ ALMACENES - CORREGIDO
    // =======================
    if (path === "/almacenes") {
      const { conn, region } = await getConnection();

      try {
        const r = await conn.execute(
          `SELECT a.id, a.nombre, a.ciudad, a.estado, a.pais, a.region,
                  (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='PENDIENTE') AS pendientes,
                  (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='EN_TRANSITO') AS transito,
                  (SELECT COUNT(*) FROM envios WHERE almacen_destino_id = a.id AND estatus='ENTREGADO') AS entregados
           FROM almacenes a
           WHERE a.activo = 1
           ORDER BY a.id`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        await conn.close();
        return res.json({ data: r.rows || [], region });
      } catch (err) {
        await conn.close();
        throw err;
      }
    }

    // =======================
    // ✅ VIAJES - CORREGIDO
    // =======================
    if (path === "/viajes") {
      const { conn, region } = await getConnection();

      try {
        const r = await conn.execute(
          `SELECT v.id, v.responsable_nombre, u.placa, r.nombre AS ruta,
                  v.estatus,
                  (SELECT COUNT(*) FROM envios WHERE viaje_id = v.id) AS paquetes
           FROM viajes v
           JOIN unidades u ON u.id = v.unidad_id
           JOIN rutas r ON r.id = v.ruta_id
           ORDER BY v.id DESC`,
          [],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        await conn.close();
        return res.json({ data: r.rows || [], region });
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
      details: "Error conectando a base de datos"
    });
  }
};