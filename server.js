const express = require('express');
const path = require('path');
const apiHandler = require('./api/index');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Servir archivos estáticos
app.use(express.static(path.join(__dirname, 'public')));

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// API routes
app.all('/api/*', async (req, res) => {
  try {
    await apiHandler(req, res);
  } catch (error) {
    console.error('API Error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Servir index.html para todas las demás rutas
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Iniciar servidor
const server = app.listen(PORT, '0.0.0.0', async () => {
  console.log(`🚀 Servidor corriendo en puerto ${PORT}`);
  console.log(`📍 URL: http://localhost:${PORT}`);
  
  // Inicializar pools de base de datos
  console.log('🔄 Inicializando conexiones a bases de datos...');
  try {
    const response = await fetch(`http://localhost:${PORT}/api/init`);
    const result = await response.json();
    console.log('✅ Inicialización completada:', result);
  } catch (err) {
    console.log('⚠️ Error en inicialización:', err.message);
  }
});

// Manejo de cierre graceful
process.on('SIGTERM', () => {
  console.log('SIGTERM recibido, cerrando servidor...');
  server.close(() => {
    console.log('Servidor cerrado');
    process.exit(0);
  });
});