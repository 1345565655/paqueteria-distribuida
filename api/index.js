// ============================================
// API COMPLETA PARA VERCEL
// Archivo: api/index.js
// ============================================

const oracledb = require('oracledb');

// Configuración de Oracle Cloud
const dbConfigMexico = {
  user: 'ADMIN',
  password: 'Adi4APH_827HK',
  connectString: 'adb.mx-queretaro-1.oraclecloud.com:1522/g1eba54685c8450_dbmexico_high.adb.oraclecloud.com'
};

const dbConfigCanada = {
  user: 'ADMIN',
  password: 'Adi4APH_827HK',
  connectString: 'adb.mx-queretaro-1.oraclecloud.com:1522/g1eba54685c8450_dbcanada_high.adb.oraclecloud.com'
};

// Pool de conexiones
let poolMexico = null;
let poolCanada = null;

// Inicializar pools
async function initPools() {
  try {
    if (!poolMexico) {
      poolMexico = await oracledb.createPool({
        ...dbConfigMexico,
        poolMin: 1,
        poolMax: 10,
        poolIncrement: 1
      });
      console.log('✅ Pool México creado');
    }
  } catch (err) {
    console.log('❌ Pool México falló:', err.message);
  }

  try {
    if (!poolCanada) {
      poolCanada = await oracledb.createPool({
        ...dbConfigCanada,
        poolMin: 1,
        poolMax: 10,
        poolIncrement: 1
      });
      console.log('✅ Pool Canadá creado');
    }
  } catch (err) {
    console.log('❌ Pool Canadá falló:', err.message);
  }
}

// Obtener conexión disponible (tolerancia a fallos)
async function getConnection() {
  await initPools();
  
  // Intentar México primero
  if (poolMexico) {
    try {
      const conn = await poolMexico.getConnection();
      return { conn, region: 'MEXICO', pool: poolMexico };
    } catch (err) {
      console.log('⚠️ México no disponible, intentando Canadá...');
    }
  }

  // Si México falla, usar Canadá
  if (poolCanada) {
    try {
      const conn = await poolCanada.getConnection();
      return { conn, region: 'CANADA', pool: poolCanada };
    } catch (err) {
      console.log('❌ Canadá tampoco disponible');
    }
  }

  throw new Error('Ninguna base de datos disponible');
}

// Replicar datos entre regiones
async function replicarDatos(query, binds, regionOrigen) {
  const regionDestino = regionOrigen === 'MEXICO' ? 'CANADA' : 'MEXICO';
  const poolDestino = regionDestino === 'MEXICO' ? poolMexico : poolCanada;

  if (!poolDestino) return;

  try {
    const conn = await poolDestino.getConnection();
    await conn.execute(query, binds, { autoCommit: true });
    await conn.close();
    console.log(`✅ Datos replicados a ${regionDestino}`);
  } catch (err) {
    console.log(`⚠️ No se pudo replicar a ${regionDestino}: ${err.message}`);
  }
}

// Handler principal para Vercel
module.exports = async (req, res) => {
  // Habilitar CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const { method, url } = req;
  const path = url.split('?')[0];

  try {
    // ========== ESTADO DEL SISTEMA ==========
    if (path === '/api/status' && method === 'GET') {
      await initPools();
      return res.json({
        mexico: poolMexico ? 'CONECTADA' : 'DESCONECTADA',
        canada: poolCanada ? 'CONECTADA' : 'DESCONECTADA'
      });
    }

    // ========== OBTENER ENVÍOS ==========
    if (path === '/api/envios' && method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT e.id, e.numero_guia, 
                c.nombre || ' ' || c.apellidos as cliente,
                ao.ciudad as origen, ad.ciudad as destino,
                e.estatus, e.costo_envio, e.fecha_creacion
         FROM envios e
         JOIN clientes c ON e.cliente_id = c.id
         JOIN almacenes ao ON e.almacen_origen_id = ao.id
         JOIN almacenes ad ON e.almacen_destino_id = ad.id
         ORDER BY e.fecha_creacion DESC
         FETCH FIRST 50 ROWS ONLY`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();
      
      return res.json({
        data: result.rows,
        region,
        timestamp: new Date().toISOString()
      });
    }

    // ========== CREAR ENVÍO ==========
    if (path === '/api/envios' && method === 'POST') {
      const { conn, region } = await getConnection();
      const body = req.body;

      const insertQuery = `
        INSERT INTO envios (
          numero_guia, cliente_id, almacen_origen_id, almacen_destino_id,
          descripcion, peso_kg, estatus, costo_envio, 
          destinatario_nombre, destinatario_telefono,
          region_origen, region_destino
        ) VALUES (
          :numero_guia, :cliente_id, :almacen_origen_id, :almacen_destino_id,
          :descripcion, :peso_kg, 'PENDIENTE', :costo_envio,
          :destinatario_nombre, :destinatario_telefono,
          :region_origen, :region_destino
        ) RETURNING id INTO :id`;

      const binds = {
        numero_guia: `ENV-${Date.now()}`,
        cliente_id: body.cliente_id,
        almacen_origen_id: body.almacen_origen_id,
        almacen_destino_id: body.almacen_destino_id,
        descripcion: body.descripcion,
        peso_kg: body.peso_kg,
        costo_envio: body.costo_envio,
        destinatario_nombre: body.destinatario_nombre,
        destinatario_telefono: body.destinatario_telefono,
        region_origen: body.region_origen || region,
        region_destino: body.region_destino || region,
        id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
      };

      const result = await conn.execute(insertQuery, binds, { autoCommit: true });
      await conn.close();

      // Replicar a otra región
      await replicarDatos(insertQuery, binds, region);

      return res.json({
        success: true,
        id: result.outBinds.id[0],
        region,
        message: 'Envío creado y replicado'
      });
    }

    // ========== OBTENER CLIENTES ==========
    if (path === '/api/clientes' && method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT c.id, c.nombre, c.apellidos, c.email, c.telefono,
                c.ciudad, c.pais, c.region,
                COUNT(e.id) as total_envios,
                NVL(SUM(p.monto), 0) as total_pagado
         FROM clientes c
         LEFT JOIN envios e ON c.id = e.cliente_id
         LEFT JOIN pagos p ON e.id = p.envio_id
         GROUP BY c.id, c.nombre, c.apellidos, c.email, c.telefono,
                  c.ciudad, c.pais, c.region
         ORDER BY c.fecha_registro DESC`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();

      return res.json({
        data: result.rows,
        region
      });
    }

    // ========== CREAR CLIENTE ==========
    if (path === '/api/clientes' && method === 'POST') {
      const { conn, region } = await getConnection();
      const body = req.body;

      const insertQuery = `
        INSERT INTO clientes (
          nombre, apellidos, email, telefono, 
          direccion, ciudad, estado, pais, region
        ) VALUES (
          :nombre, :apellidos, :email, :telefono,
          :direccion, :ciudad, :estado, :pais, :region
        ) RETURNING id INTO :id`;

      const binds = {
        nombre: body.nombre,
        apellidos: body.apellidos,
        email: body.email,
        telefono: body.telefono,
        direccion: body.direccion,
        ciudad: body.ciudad,
        estado: body.estado,
        pais: body.pais || 'México',
        region: body.region || region,
        id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
      };

      const result = await conn.execute(insertQuery, binds, { autoCommit: true });
      await conn.close();

      // Replicar
      await replicarDatos(insertQuery, binds, region);

      return res.json({
        success: true,
        id: result.outBinds.id[0],
        region,
        message: 'Cliente creado y replicado'
      });
    }

    // ========== OBTENER ALMACENES ==========
    if (path === '/api/almacenes' && method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT a.id, a.nombre, a.ciudad, a.estado, a.pais, a.region,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus = 'PENDIENTE') as paquetes_espera,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus = 'EN_TRANSITO') as en_transito,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus = 'ENTREGADO') as enviados
         FROM almacenes a
         WHERE a.activo = 1
         ORDER BY a.region, a.nombre`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();

      return res.json({
        data: result.rows,
        region
      });
    }

    // ========== OBTENER VIAJES ==========
    if (path === '/api/viajes' && method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT v.id, v.responsable_nombre, u.placa, 
                r.nombre as ruta, v.estatus,
                (SELECT COUNT(*) FROM envios WHERE viaje_id = v.id) as total_paquetes
         FROM viajes v
         JOIN unidades u ON v.unidad_id = u.id
         JOIN rutas r ON v.ruta_id = r.id
         ORDER BY v.fecha_creacion DESC
         FETCH FIRST 20 ROWS ONLY`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();

      return res.json({
        data: result.rows,
        region
      });
    }

    // ========== ACTUALIZAR ESTATUS ENVÍO ==========
    if (path.startsWith('/api/envios/') && path.includes('/estatus') && method === 'PUT') {
      const envioId = path.split('/')[3];
      const { conn, region } = await getConnection();
      const { estatus } = req.body;

      const updateQuery = `
        UPDATE envios 
        SET estatus = :estatus,
            fecha_entrega_real = CASE WHEN :estatus = 'ENTREGADO' THEN SYSTIMESTAMP ELSE fecha_entrega_real END
        WHERE id = :id`;

      const binds = { estatus, id: envioId };

      await conn.execute(updateQuery, binds, { autoCommit: true });
      await conn.close();

      // Replicar
      await replicarDatos(updateQuery, binds, region);

      return res.json({
        success: true,
        region,
        message: 'Estatus actualizado y replicado'
      });
    }

    // ========== DASHBOARD STATS ==========
    if (path === '/api/dashboard' && method === 'GET') {
      const { conn, region } = await getConnection();
      
      const stats = await conn.execute(
        `SELECT 
          (SELECT COUNT(*) FROM envios WHERE estatus IN ('PENDIENTE', 'EN_TRANSITO')) as envios_activos,
          (SELECT COUNT(*) FROM clientes WHERE activo = 1) as total_clientes,
          (SELECT COUNT(*) FROM almacenes WHERE activo = 1) as total_almacenes,
          (SELECT COUNT(*) FROM viajes WHERE estatus IN ('PROGRAMADO', 'EN_CURSO')) as viajes_activos
         FROM dual`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      
      await conn.close();

      return res.json({
        data: stats.rows[0],
        region
      });
    }

    return res.status(404).json({ error: 'Ruta no encontrada' });

  } catch (error) {
    console.error('Error:', error);
    return res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
};