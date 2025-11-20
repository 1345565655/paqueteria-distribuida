const oracledb = require('oracledb');

// Configuración desde variables de entorno
const dbConfigMexico = {
  user: process.env.DB_MEXICO_USER || 'ADMIN',
  password: process.env.DB_MEXICO_PASSWORD || 'Adi4APH_827HK',
  connectString: `${process.env.DB_MEXICO_HOST || 'adb.mx-queretaro-1.oraclecloud.com'}:${process.env.DB_MEXICO_PORT || '1522'}/${process.env.DB_MEXICO_SERVICE || 'g1eba54685c8450_dbmexico_high.adb.oraclecloud.com'}`
};

const dbConfigCanada = {
  user: process.env.DB_CANADA_USER || 'ADMIN',
  password: process.env.DB_CANADA_PASSWORD || 'Adi4APH_827HK',
  connectString: `${process.env.DB_CANADA_HOST || 'adb.mx-queretaro-1.oraclecloud.com'}:${process.env.DB_CANADA_PORT || '1522'}/${process.env.DB_CANADA_SERVICE || 'g1eba54685c8450_dbcanada_high.adb.oraclecloud.com'}`
};

let poolMexico = null;
let poolCanada = null;

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

async function getConnection() {
  await initPools();
  
  if (poolMexico) {
    try {
      const conn = await poolMexico.getConnection();
      return { conn, region: 'MEXICO', pool: poolMexico };
    } catch (err) {
      console.log('⚠️ México no disponible, intentando Canadá...');
    }
  }

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

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const path = req.url.replace('/api', '');

  try {
    // Status
    if (path === '/status' && req.method === 'GET') {
      await initPools();
      return res.json({
        mexico: poolMexico ? 'CONECTADA' : 'DESCONECTADA',
        canada: poolCanada ? 'CONECTADA' : 'DESCONECTADA'
      });
    }

    // Dashboard
    if (path === '/dashboard' && req.method === 'GET') {
      const { conn, region } = await getConnection();
      const stats = await conn.execute(
        `SELECT 
          (SELECT COUNT(*) FROM envios WHERE estatus IN ('PENDIENTE', 'EN_TRANSITO')) as ENVIOS_ACTIVOS,
          (SELECT COUNT(*) FROM clientes WHERE activo = 1) as TOTAL_CLIENTES,
          (SELECT COUNT(*) FROM almacenes WHERE activo = 1) as TOTAL_ALMACENES,
          (SELECT COUNT(*) FROM viajes WHERE estatus IN ('PROGRAMADO', 'EN_CURSO')) as VIAJES_ACTIVOS
         FROM dual`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();
      return res.json({ data: stats.rows[0], region });
    }

    // Envíos
    if (path === '/envios' && req.method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT e.id, e.numero_guia, 
                c.nombre || ' ' || c.apellidos as CLIENTE,
                ao.ciudad as ORIGEN, ad.ciudad as DESTINO,
                e.estatus, e.costo_envio as COSTO_ENVIO
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
      return res.json({ data: result.rows, region });
    }

    // Clientes
    if (path === '/clientes' && req.method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT c.id, c.nombre, c.apellidos, c.email, c.telefono,
                c.ciudad, c.pais, c.region,
                COUNT(e.id) as TOTAL_ENVIOS,
                NVL(SUM(p.monto), 0) as TOTAL_PAGADO
         FROM clientes c
         LEFT JOIN envios e ON c.id = e.cliente_id
         LEFT JOIN pagos p ON e.id = p.envio_id
         GROUP BY c.id, c.nombre, c.apellidos, c.email, c.telefono, c.ciudad, c.pais, c.region
         ORDER BY c.fecha_registro DESC`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();
      return res.json({ data: result.rows, region });
    }

    // Almacenes
    if (path === '/almacenes' && req.method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT a.id, a.nombre, a.ciudad, a.estado, a.pais, a.region,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus = 'PENDIENTE') as PAQUETES_ESPERA,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus = 'EN_TRANSITO') as EN_TRANSITO,
                (SELECT COUNT(*) FROM envios WHERE almacen_origen_id = a.id AND estatus = 'ENTREGADO') as ENVIADOS
         FROM almacenes a WHERE a.activo = 1
         ORDER BY a.region, a.nombre`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();
      return res.json({ data: result.rows, region });
    }

    // Viajes
    if (path === '/viajes' && req.method === 'GET') {
      const { conn, region } = await getConnection();
      const result = await conn.execute(
        `SELECT v.id, v.responsable_nombre, u.placa, r.nombre as RUTA, v.estatus,
                (SELECT COUNT(*) FROM envios WHERE viaje_id = v.id) as TOTAL_PAQUETES
         FROM viajes v
         JOIN unidades u ON v.unidad_id = u.id
         JOIN rutas r ON v.ruta_id = r.id
         ORDER BY v.fecha_creacion DESC FETCH FIRST 20 ROWS ONLY`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      await conn.close();
      return res.json({ data: result.rows, region });
    }

    return res.status(404).json({ error: 'Ruta no encontrada' });

  } catch (error) {
    console.error('Error:', error);
    return res.status(500).json({ error: error.message });
  }
};