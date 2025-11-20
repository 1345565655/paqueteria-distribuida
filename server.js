const express = require("express");
const bodyParser = require("body-parser");
const expressLayouts = require("express-ejs-layouts");

const {
  dbMexico,
  dbCanada,
  rowsToObjects,
  oracledb
} = require("./config/db");

// ======================
// INICIAR APP
// ======================
const app = express();
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());

app.set("view engine", "ejs");
app.use(expressLayouts);
app.set("layout", "layout");

// ======================
// CONEXIÓN SEGURA
// ======================
async function tryConn(conf) {
  try {
    return await oracledb.getConnection(conf);
  } catch (err) {
    return null;
  }
}

// ======================
// RUTAS DE MENÚ
// ======================
app.get("/", (req, res) => res.render("index"));
app.get("/clientes", (req, res) => res.render("clientes"));

// ======================
// FORMULARIO DE ENVÍO
// ======================
app.get("/envios", async (req, res) => {
  const conn =
    (await tryConn(dbMexico)) ||
    (await tryConn(dbCanada));

  if (!conn)
    return res.send("❌ No hay conexión con ninguna BD");

  const clientesR = await conn.execute(`
    SELECT id, nombre, apellidos
    FROM clientes
    ORDER BY nombre
  `);

  const almacenesR = await conn.execute(`
    SELECT id, nombre
    FROM almacenes
    ORDER BY nombre
  `);

  const clientes = rowsToObjects(clientesR);
  const almacenes = rowsToObjects(almacenesR);

  await conn.close();
  res.render("envios", { clientes, almacenes });
});

app.get("/tracking", (req, res) => res.render("tracking"));
app.get("/replicar", (req, res) => res.render("replicar"));

app.get("/reportes", (req, res) => res.render("reportes_menu"));

// ======================
// GUARDAR CLIENTE
// ======================
app.post("/clientes/nuevo", async (req, res) => {
  const { Nombre, Apellidos, Email, Telefono } = req.body;

  const sql = `
    INSERT INTO clientes(nombre, apellidos, email, telefono)
    VALUES(:n, :a, :e, :t)
  `;

  try {
    let conn = await tryConn(dbMexico);
    let destino = "México";

    if (!conn) {
      conn = await tryConn(dbCanada);
      destino = "Canadá";
    }

    await conn.execute(
      sql,
      { n: Nombre, a: Apellidos, e: Email, t: Telefono },
      { autoCommit: true }
    );

    await conn.close();

    res.send(`<script>alert("Cliente guardado en ${destino}"); window.location="/";</script>`);
  } catch (err) {
    res.send(err.message);
  }
});

// ======================
// GUARDAR ENVÍO
// ======================
app.post("/envios/nuevo", async (req, res) => {
  const { cliente_id, almacen_origen, almacen_destino, peso, costo } = req.body;

  const sql = `
    INSERT INTO envios(
      numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
      peso_kg, costo_envio, fecha_creacion, estatus
    )
    VALUES(
      'GUIA-'||TO_CHAR(SYSTIMESTAMP,'YYYYMMDDHH24MISS'),
      :c, :o, :d, :p, :cost, SYSTIMESTAMP, 'PENDIENTE'
    )
  `;

  try {
    let conn = await tryConn(dbMexico);
    let destino = "México";

    if (!conn) {
      conn = await tryConn(dbCanada);
      destino = "Canadá";
    }

    await conn.execute(
      sql,
      { c: cliente_id, o: almacen_origen, d: almacen_destino, p: peso, cost: costo },
      { autoCommit: true }
    );

    await conn.close();

    res.send(`<script>alert("Envío guardado en ${destino}"); window.location="/";</script>`);
  } catch (err) {
    res.send(err.message);
  }
});

// ==============================
// REPORTE DE CLIENTES
// ==============================
app.get("/reportes/clientes", async (req, res) => {
  const conn = await tryConn(dbMexico) || await tryConn(dbCanada);

  if (!conn)
    return res.send("❌ No hay conexión con ninguna BD");

  const r = await conn.execute(`
    SELECT nombre, apellidos, email, telefono
    FROM clientes
  `);

  const data = rowsToObjects(r);
  await conn.close();

  res.render("reportes_clientes", { data });
});

// ==============================
// REPORTE DE ENVÍOS
// ==============================
app.get("/reportes/envios", async (req, res) => {
  const conn = await tryConn(dbMexico) || await tryConn(dbCanada);

  if (!conn)
    return res.send("❌ No hay conexión con ninguna BD");

  const r = await conn.execute(`
    SELECT numero_guia, cliente_id, almacen_origen_id,
           almacen_destino_id, peso_kg, costo_envio,
           fecha_creacion, estatus
    FROM envios
    ORDER BY fecha_creacion DESC
  `);

  const data = rowsToObjects(r);
  await conn.close();

  res.render("reportes_envios", { data });
});

// ==============================
// REPORTE DE RUTAS
// ==============================
app.get("/reportes/rutas", async (req, res) => {
  const conn = await tryConn(dbMexico) || await tryConn(dbCanada);

  if (!conn)
    return res.send("❌ No hay conexión con ninguna BD");

  const r = await conn.execute(`
    SELECT numero_guia, estatus, fecha_envio, fecha_entrega_real
    FROM envios
    ORDER BY fecha_envio DESC
  `);

  const data = rowsToObjects(r);
  await conn.close();

  res.render("reportes_rutas", { data });
});

// ======================
// CONSULTAR TRACKING
// ======================
app.get("/tracking/resultado", async (req, res) => {
  const guia = req.query.guia;

  try {
    let conn = await tryConn(dbMexico) || await tryConn(dbCanada);

    const r = await conn.execute(
      `SELECT numero_guia, estatus, fecha_envio, fecha_entrega_real
       FROM envios WHERE numero_guia = :g`,
      { g: guia }
    );

    const data = rowsToObjects(r);
    conn.close();

    if (data.length === 0)
      return res.send(`<h1>No existe seguimiento para ${guia}</h1>`);

    return res.render("tracking_result", { data: data[0] });

  } catch (err) {
    res.send(err.message);
  }
});

// ===================================================================
// ================== REPLICACIÓN (FUNCIONANDO CON DB LINKS) =========
// ===================================================================
// ==============================
// REPlicación usando DB LINKS
// ==============================

// NOMBRES DE DB LINKS
const LINK_CANADA = "LINK_CANADA";
const LINK_MEXICO = "LINK_MEXICO";

// 1) CLIENTES MX -> CA (se ejecuta en México)
const SQL_CLIENTES_MX_TO_CA = `
INSERT INTO clientes@${LINK_CANADA} (
  nombre, apellidos, email, telefono, fecha_nacimiento,
  direccion, ciudad, estado, pais, region
)
SELECT nombre, apellidos, email, telefono, fecha_nacimiento,
       direccion, ciudad, estado, pais, region
FROM clientes c
WHERE NOT EXISTS (
  SELECT 1 FROM clientes@${LINK_CANADA} cc
  WHERE cc.email = c.email
)
`;

// 2) CLIENTES CA -> MX (se ejecuta en Canadá)
const SQL_CLIENTES_CA_TO_MX = `
INSERT INTO clientes@${LINK_MEXICO} (
  nombre, apellidos, email, telefono, fecha_nacimiento,
  direccion, ciudad, estado, pais, region
)
SELECT nombre, apellidos, email, telefono, fecha_nacimiento,
       direccion, ciudad, estado, pais, region
FROM clientes c
WHERE NOT EXISTS (
  SELECT 1 FROM clientes@${LINK_MEXICO} cm
  WHERE cm.email = c.email
)
`;

// 3) ENVÍOS MX -> CA (se ejecuta en México)
const SQL_ENVIOS_MX_TO_CA = `
INSERT INTO envios@${LINK_CANADA} (
  numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
  viaje_id, descripcion, peso_kg, dimensiones, valor_declarado,
  estatus, fecha_creacion, fecha_envio, fecha_entrega_estimada,
  fecha_entrega_real, costo_envio, costo_adicional,
  destinatario_nombre, destinatario_telefono, destinatario_direccion,
  region_origen, region_destino
)
SELECT
  numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
  viaje_id, descripcion, peso_kg, dimensiones, valor_declarado,
  estatus, fecha_creacion, fecha_envio, fecha_entrega_estimada,
  fecha_entrega_real, costo_envio, costo_adicional,
  destinatario_nombre, destinatario_telefono, destinatario_direccion,
  region_origen, region_destino
FROM envios e
WHERE NOT EXISTS (
  SELECT 1 FROM envios@${LINK_CANADA} ec
  WHERE ec.numero_guia = e.numero_guia
)
`;

// 4) ENVÍOS CA -> MX (se ejecuta en Canadá)
const SQL_ENVIOS_CA_TO_MX = `
INSERT INTO envios@${LINK_MEXICO} (
  numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
  viaje_id, descripcion, peso_kg, dimensiones, valor_declarado,
  estatus, fecha_creacion, fecha_envio, fecha_entrega_estimada,
  fecha_entrega_real, costo_envio, costo_adicional,
  destinatario_nombre, destinatario_telefono, destinatario_direccion,
  region_origen, region_destino
)
SELECT
  numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
  viaje_id, descripcion, peso_kg, dimensiones, valor_declarado,
  estatus, fecha_creacion, fecha_envio, fecha_entrega_estimada,
  fecha_entrega_real, costo_envio, costo_adicional,
  destinatario_nombre, destinatario_telefono, destinatario_direccion,
  region_origen, region_destino
FROM envios e
WHERE NOT EXISTS (
  SELECT 1 FROM envios@${LINK_MEXICO} em
  WHERE em.numero_guia = e.numero_guia
)
`;

// ==========================================
// FUNCIÓN PRINCIPAL DE REPLICACIÓN COMPLETA
// ==========================================
async function replicarTodo() {
  console.log("⏳ Ejecutando replicación via DB LINKS...");

  // Conexiones MX y CA
  const mx = await tryConn(dbMexico);
  const ca = await tryConn(dbCanada);

  if (!mx || !ca) {
    console.log("❌ No hay conexión con alguna BD (MX o CA).");
    if (mx) await mx.close().catch(() => {});
    if (ca) await ca.close().catch(() => {});
    return;
  }

  try {
    // MX → CA
    console.log("-> CLIENTES MX -> CA...");
    await mx.execute(SQL_CLIENTES_MX_TO_CA, [], { autoCommit: true });

    console.log("-> ENVIOS MX -> CA...");
    await mx.execute(SQL_ENVIOS_MX_TO_CA, [], { autoCommit: true });

    // CA → MX
    console.log("-> CLIENTES CA -> MX...");
    await ca.execute(SQL_CLIENTES_CA_TO_MX, [], { autoCommit: true });

    console.log("-> ENVIOS CA -> MX...");
    await ca.execute(SQL_ENVIOS_CA_TO_MX, [], { autoCommit: true });

    console.log("✔ Replicación via DB LINKS finalizada");
  } catch (err) {
    console.error("❌ Error durante replicación via DB LINKS:", err.message);
  } finally {
    try { await mx.close(); } catch (e) {}
    try { await ca.close(); } catch (e) {}
  }
}

// =======================
// RUTA PARA EJECUTAR MANUAL
// =======================
app.get("/replicar/run", async (req, res) => {
  await replicarTodo();
  res.send(`
    <script>
      alert("Replicación ejecutada (DB LINKS). Revisa logs en consola.");
      window.location="/";
    </script>
  `);
});

// ==========================
// CRON AUTOMÁTICO (cada 2 min)
// ==========================
setInterval(replicarTodo, 1000 * 60 * 2);


// ===================================================================
// INICIAR SERVIDOR
// ===================================================================
const PORT = 3000;
app.listen(PORT, () =>
  console.log(`🔥 Servidor en http://localhost:${PORT}`)
);
