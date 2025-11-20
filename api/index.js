const oracledb = require("oracledb");

// =============================
// CONFIG DB
// =============================
const dbConfigMexico = {
  user: process.env.DB_MEXICO_USER || "ADMIN",
  password: process.env.DB_MEXICO_PASSWORD || "Adi4APH_827HK",
  connectString:
    `${process.env.DB_MEXICO_HOST || "adb.mx-queretaro-1.oraclecloud.com"}:` +
    `${process.env.DB_MEXICO_PORT || "1522"}/` +
    `${process.env.DB_MEXICO_SERVICE || "g1eba54685c8450_dbmexico_high.adb.oraclecloud.com"}`
};

const dbConfigCanada = {
  user: process.env.DB_CANADA_USER || "ADMIN",
  password: process.env.DB_CANADA_PASSWORD || "Adi4APH_827HK",
  connectString:
    `${process.env.DB_CANADA_HOST || "adb.mx-queretaro-1.oraclecloud.com"}:` +
    `${process.env.DB_CANADA_PORT || "1522"}/` +
    `${process.env.DB_CANADA_SERVICE || "g1eba54685c8450_dbcanada_high.adb.oraclecloud.com"}`
};

let poolMexico = null;
let poolCanada = null;

// =============================
// Crear pools
// =============================
async function initPools() {
  try {
    if (!poolMexico) {
      poolMexico = await oracledb.createPool({
        ...dbConfigMexico,
        poolMin: 1,
        poolMax: 10,
        poolIncrement: 1
      });
      console.log("🇲🇽 Pool México iniciado");
    }
  } catch (err) {
    console.log("❌ Error pool México:", err.message);
  }

  try {
    if (!poolCanada) {
      poolCanada = await oracledb.createPool({
        ...dbConfigCanada,
        poolMin: 1,
        poolMax: 10,
        poolIncrement: 1
      });
      console.log("🇨🇦 Pool Canadá iniciado");
    }
  } catch (err) {
    console.log("❌ Error pool Canadá:", err.message);
  }
}

// =============================
// Obtener conexión automática
// =============================
async function getConnection() {
  await initPools();

  // 1️⃣ Intentar México primero
  if (poolMexico) {
    try {
      const conn = await poolMexico.getConnection();
      return { conn, region: "MEXICO" };
    } catch {}
  }

  // 2️⃣ Intentar Canadá
  if (poolCanada) {
    try {
      const conn = await poolCanada.getConnection();
      return { conn, region: "CANADA" };
    } catch {}
  }

  throw new Error("❌ Ninguna base de datos disponible");
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
    console.log(`🔁 Replicado en ${destino}`);
  } catch (err) {
    console.log(`⚠ No replicado en ${destino}: ${err.message}`);
  }
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
    // ✔ STATUS
    // =======================
    if (path === "/status") {
      await initPools();
      return res.json({
        mexico: poolMexico ? "CONECTADA" : "DESCONECTADA",
        canada: poolCanada ? "CONECTADA" : "DESCONECTADA"
      });
    }

    // =======================
    // ✔ DASHBOARD
    // =======================
    if (path === "/dashboard") {
      const { conn, region } = await getConnection();

      const stats = await conn.execute(
        `SELECT 
          (SELECT COUNT(*) FROM envios WHERE estatus IN ('PENDIENTE','EN_TRANSITO')) AS ENVIOS_ACTIVOS,
          (SELECT COUNT(*) FROM clientes WHERE activo = 1) AS TOTAL_CLIENTES,
          (SELECT COUNT(*) FROM almacenes WHERE activo = 1) AS TOTAL_ALMACENES,
          (SELECT COUNT(*) FROM viajes WHERE estatus IN ('PROGRAMADO','EN_CURSO')) AS VIAJES_ACTIVOS
        FROM dual`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      await conn.close();
      return res.json({ data: stats.rows[0], region });
    }

    // =======================
    // ✔ GET /envios
    // =======================
    if (path === "/envios" && req.method === "GET") {
      const { conn, region } = await getConnection();

      const r = await conn.execute(
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
      );

      await conn.close();
      return res.json({ data: r.rows, region });
    }

    // =======================
    // ✔ POST /envios  (FALTABA)
    // =======================
    if (path === "/envios" && req.method === "POST") {
      const body = await new Promise((resolve) => {
        let b = "";
        req.on("data", (d) => (b += d.toString()));
        req.on("end", () => resolve(JSON.parse(b || "{}")));
      });

      const { conn, region } = await getConnection();

      const numeroGuia = "MX-" + Date.now().toString(36).toUpperCase();

      const query = `
        INSERT INTO envios 
        (id, numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
         descripcion, peso_kg, costo_envio, destinatario_nombre,
         destinatario_telefono, estatus)
        VALUES (envios_seq.NEXTVAL, :guia, :cliente, :origen, :destino,
                :descripcion, :peso, :costo, :destinatario, :telefono, 'PENDIENTE')
      `;

      const binds = {
        guia: numeroGuia,
        cliente: body.cliente_id,
        origen: body.almacen_origen_id,
        destino: body.almacen_destino_id,
        descripcion: body.descripcion,
        peso: body.peso_kg,
        costo: body.costo_envio,
        destinatario: body.destinatario_nombre,
        telefono: body.destinatario_telefono
      };

      await conn.execute(query, binds, { autoCommit: true });
      await conn.close();

      // replicación
      replicar(query, binds, region);

      return res.json({ success: true, numeroGuia });
    }

    // =======================
    // ✔ CLIENTES
    // =======================
    if (path === "/clientes") {
      const { conn, region } = await getConnection();

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
      return res.json({ data: r.rows, region });
    }

    // =======================
    // ✔ ALMACENES
    // =======================
    if (path === "/almacenes") {
      const { conn, region } = await getConnection();

      const r = await conn.execute(
        `SELECT a.id, a.nombre, a.ciudad, a.estado, a.pais, a.region,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='PENDIENTE') AS pendientes,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='EN_TRANSITO') AS transito,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus='ENTREGADO') AS entregados
         FROM almacenes a
         WHERE a.activo = 1
         ORDER BY a.id DESC`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      await conn.close();
      return res.json({ data: r.rows, region });
    }

    // =======================
    // ✔ VIAJES
    // =======================
    if (path === "/viajes") {
      const { conn, region } = await getConnection();

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
      return res.json({ data: r.rows, region });
    }

    return res.status(404).json({ error: "Ruta no encontrada" });

  } catch (err) {
    console.log("🔥 ERROR:", err);
    return res.status(500).json({ error: err.message });
  }
};
