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

  // Traemos clientes
  const clientesR = await conn.execute(`
    SELECT id, nombre, apellidos
    FROM clientes
    ORDER BY nombre
  `);
  const clientes = rowsToObjects(clientesR);

  // Traemos almacenes
  const almacenesR = await conn.execute(`
    SELECT id, nombre
    FROM almacenes
    ORDER BY nombre
  `);
  const almacenes = rowsToObjects(almacenesR);

  await conn.close();

  res.render("envios", { clientes, almacenes });
});
app.get("/tracking", (req, res) => res.render("tracking"));
app.get("/replicar", (req, res) => res.render("replicar"));
app.get("/reportes", (req, res) => {
  res.render("reportes_menu");
});

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

    await conn.execute(sql, {
      c: cliente_id,
      o: almacen_origen,
      d: almacen_destino,
      p: peso,
      cost: costo
    }, { autoCommit: true });

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
  const conn =
    (await tryConn(dbMexico)) ||
    (await tryConn(dbCanada));

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
  const conn =
    (await tryConn(dbMexico)) ||
    (await tryConn(dbCanada));

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
// REPORTE DE RUTAS (TRACKING)
// ==============================
app.get("/reportes/rutas", async (req, res) => {
  const conn =
    (await tryConn(dbMexico)) ||
    (await tryConn(dbCanada));

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

  const sql = `
    SELECT numero_guia, estatus, fecha_envio, fecha_entrega_real
    FROM envios
    WHERE numero_guia = :g
  `;

  try {
    let conn = await tryConn(dbMexico) || await tryConn(dbCanada);
    const r = await conn.execute(sql, { g: guia });
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
// ======================== FUNCIONES DE REPLICA =====================
// ===================================================================

// CLIENTES MX → CA
async function replicarClientes_MX_CA(mx, ca) {
  const all = await mx.execute(
    `SELECT nombre, apellidos, email, telefono FROM clientes`
  );
  let n = 0;

  for (const c of all.rows) {
    const existe = await ca.execute(
      `SELECT 1 FROM clientes WHERE email = :e`,
      { e: c[2] }
    );

    if (existe.rows.length === 0) {
      await ca.execute(
        `INSERT INTO clientes(nombre, apellidos, email, telefono)
         VALUES(:n, :a, :e, :t)`,
        {
          n: c[0],
          a: c[1],
          e: c[2],
          t: c[3]
        },
        { autoCommit: true }
      );
      n++;
    }
  }

  console.log(`🟢 CLIENTES MX → CA replicados: ${n}`);
  return n;
}

// CLIENTES CA → MX
async function replicarClientes_CA_MX(mx, ca) {
  const all = await ca.execute(
    `SELECT nombre, apellidos, email, telefono FROM clientes`
  );
  let n = 0;

  for (const c of all.rows) {
    const existe = await mx.execute(
      `SELECT 1 FROM clientes WHERE email = :e`,
      { e: c[2] }
    );

    if (existe.rows.length === 0) {
      await mx.execute(
        `INSERT INTO clientes(nombre, apellidos, email, telefono)
         VALUES(:n, :a, :e, :t)`,
        {
          n: c[0],
          a: c[1],
          e: c[2],
          t: c[3]
        },
        { autoCommit: true }
      );
      n++;
    }
  }

  console.log(`🟢 CLIENTES CA → MX replicados: ${n}`);
  return n;
}


// ENVÍOS MX → CA
async function replicarEnvios_MX_CA(mx, ca) {
  const all = await mx.execute(`SELECT numero_guia FROM envios`);
  let n = 0;

  for (const r of all.rows) {
    const guia = r[0];

    const existe = await ca.execute(
      `SELECT 1 FROM envios WHERE numero_guia = :g`,
      { g: guia }
    );

    if (existe.rows.length === 0) {
      const d = await mx.execute(
        `SELECT numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
                peso_kg, costo_envio, fecha_creacion, estatus
         FROM envios WHERE numero_guia = :g`,
        { g: guia }
      );

      const obj = rowsToObjects(d)[0];

      await ca.execute(
        `INSERT INTO envios(
          numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
          peso_kg, costo_envio, fecha_creacion, estatus
        ) VALUES(
          :numero_guia, :cliente_id, :almacen_origen_id, :almacen_destino_id,
          :peso_kg, :costo_envio, :fecha_creacion, :estatus
        )`,
        obj,
        { autoCommit: true }
      );
      n++;
    }
  }

  console.log(`📦 ENVÍOS MX → CA replicados: ${n}`);
  return n;
}


// ENVÍOS CA → MX
async function replicarEnvios_CA_MX(mx, ca) {
  const all = await ca.execute(`SELECT numero_guia FROM envios`);
  let n = 0;

  for (const r of all.rows) {
    const guia = r[0];

    const existe = await mx.execute(
      `SELECT 1 FROM envios WHERE numero_guia = :g`,
      { g: guia }
    );

    if (existe.rows.length === 0) {
      const d = await ca.execute(
        `SELECT numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
                peso_kg, costo_envio, fecha_creacion, estatus
         FROM envios WHERE numero_guia = :g`,
        { g: guia }
      );

      const obj = rowsToObjects(d)[0];

      await mx.execute(
        `INSERT INTO envios(
          numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
          peso_kg, costo_envio, fecha_creacion, estatus
        ) VALUES(
          :numero_guia, :cliente_id, :almacen_origen_id, :almacen_destino_id,
          :peso_kg, :costo_envio, :fecha_creacion, :estatus
        )`,
        obj,
        { autoCommit: true }
      );
      n++;
    }
  }

  console.log(`📦 ENVÍOS CA → MX replicados: ${n}`);
  return n;
}


// ===================================================================
// FUNCIÓN ÚNICA QUE EJECUTA LAS 4 REPLICACIONES
// ===================================================================
async function replicarTodo() {
  console.log("⏳ Ejecutando replicación en ambas bases…");

  const mx = await tryConn(dbMexico);
  const ca = await tryConn(dbCanada);

  if (!mx || !ca) {
    console.log("❌ No hay conexión con alguna BD");
    return;
  }

  await replicarClientes_MX_CA(mx, ca);
  await replicarClientes_CA_MX(mx, ca);
  await replicarEnvios_MX_CA(mx, ca);
  await replicarEnvios_CA_MX(mx, ca);

  await mx.close();
  await ca.close();

  console.log("✔ Replicación terminada");
}


// ===================================================================
// REPLICACIÓN MANUAL
// ===================================================================
app.get("/replicar/run", async (req, res) => {
  await replicarTodo();
  res.send(`<script>alert("Replicación completada con éxito"); window.location="/";</script>`);
});

// ===================================================================
// REPLICACIÓN AUTOMÁTICA CADA 2 MINUTOS
// ===================================================================
setInterval(replicarTodo, 1000 * 60 * 2);

// ===================================================================
// INICIAR SERVIDOR
// ===================================================================
const PORT = 3000;
app.listen(PORT, () =>
  console.log(`🔥 Servidor en http://localhost:${PORT}`)
);
