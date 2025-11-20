// config/db.js
const oracledb = require("oracledb");

// Si usas wallet de Oracle Autonomous es probable que necesites:
// process.env.TNS_ADMIN = "/ruta/al/wallet"; // DESCOMENTA y coloca tu ruta si usas wallet

const dbMexico = {
  user: "ADMIN",
  password: "Adi4APH_827HK",
  connectString:
    "(description=(retry_count=3)(retry_delay=2)" +
    "(address=(protocol=tcps)(port=1522)(host=adb.mx-queretaro-1.oraclecloud.com))" +
    "(connect_data=(service_name=g1eba54685c8450_dbmexico_tp.adb.oraclecloud.com))" +
    "(security=(ssl_server_dn_match=yes)))"
};

const dbCanada = {
  user: "ADMIN",
  password: "Adi4APH_827HK",
  connectString:
    "(description=(retry_count=3)(retry_delay=2)" +
    "(address=(protocol=tcps)(port=1522)(host=adb.mx-queretaro-1.oraclecloud.com))" +
    "(connect_data=(service_name=g1eba54685c8450_dbcanada_tp.adb.oraclecloud.com))" +
    "(security=(ssl_server_dn_match=yes)))"
};

// helper: convierte result.rows + metaData a array de objetos
function rowsToObjects(result) {
  if (!result || !result.rows) return [];
  const cols = (result.metaData || []).map(c => c.name);
  return result.rows.map(row => {
    const obj = {};
    for (let i = 0; i < cols.length; i++) obj[cols[i]] = row[i];
    return obj;
  });
}

async function tryConnect(conf) {
  try {
    const conn = await oracledb.getConnection(conf);
    return conn;
  } catch (e) {
    return null;
  }
}

async function getPrimaryThenReplica() {
  // intenta México, si falla regresa conexión a Canadá
  const mx = await tryConnect(dbMexico);
  if (mx) return { conn: mx, region: "MEXICO" };
  const ca = await tryConnect(dbCanada);
  if (ca) return { conn: ca, region: "CANADA" };
  throw new Error("No se pudo conectar a ninguna BD");
}

module.exports = {
  dbMexico,
  dbCanada,
  getPrimaryThenReplica,
  rowsToObjects,
  oracledb
};
