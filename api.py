from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import sqlite3
import os
from datetime import datetime

app = FastAPI(title="API MaoGo - Direcciones Valverde")

# CORS - permite acceso desde cualquier origen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Base de datos SQLite
DB_FILE = "addresses.db"

def init_db():
    """Crea la tabla si no existe"""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE NOT NULL,
            provincia TEXT,
            municipio TEXT,
            sector TEXT,
            calle TEXT,
            numero TEXT,
            referencia TEXT,
            lat REAL,
            lng REAL,
            verificado INTEGER DEFAULT 0,
            fuente TEXT,
            creado_por TEXT,
            notas TEXT,
            fecha_creacion TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

# Inicializar DB
init_db()

# Modelos
class Address(BaseModel):
    codigo: Optional[str] = None
    provincia: Optional[str] = "Valverde"
    municipio: Optional[str] = "Mao"
    sector: str
    calle: Optional[str] = None
    numero: Optional[str] = None
    referencia: Optional[str] = None
    lat: float
    lng: float
    verificado: Optional[int] = 0
    fuente: Optional[str] = "equipo"
    creado_por: Optional[str] = "API"
    notas: Optional[str] = None

class AddressUpdate(BaseModel):
    codigo: str
    provincia: Optional[str] = None
    municipio: Optional[str] = None
    sector: Optional[str] = None
    calle: Optional[str] = None
    numero: Optional[str] = None
    referencia: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    verificado: Optional[int] = None
    fuente: Optional[str] = None
    creado_por: Optional[str] = None
    notas: Optional[str] = None

# ========================================
# ENDPOINTS
# ========================================

@app.get("/")
def root():
    return {"message": "API MaoGo - Sistema de Direcciones", "version": "2.0"}

@app.get("/addresses")
def get_addresses(q: Optional[str] = None, limit: int = 500):
    """Obtiene direcciones con búsqueda opcional"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    if q:
        cur.execute("""
            SELECT * FROM addresses 
            WHERE LOWER(sector) LIKE ? OR LOWER(calle) LIKE ? 
               OR LOWER(referencia) LIKE ? OR LOWER(codigo) LIKE ?
            ORDER BY fecha_creacion DESC LIMIT ?
        """, (f"%{q.lower()}%", f"%{q.lower()}%", f"%{q.lower()}%", f"%{q.lower()}%", limit))
    else:
        cur.execute("SELECT * FROM addresses ORDER BY fecha_creacion DESC LIMIT ?", (limit,))
    
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows

@app.get("/addresses/{codigo}")
def get_address(codigo: str):
    """Obtiene una dirección por código"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM addresses WHERE codigo = ?", (codigo,))
    row = cur.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Dirección no encontrada")
    return dict(row)

@app.get("/sectors")
def get_sectors():
    """Lista de sectores únicos"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT sector FROM addresses ORDER BY sector")
    sectors = [{"sector": row[0]} for row in cur.fetchall()]
    conn.close()
    return sectors

@app.post("/addresses/insert")
def insert_address(address: Address):
    """Inserta nueva dirección con código auto-generado"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    # Generar código si no viene
    if not address.codigo:
        stop_words = {"de", "del", "la", "el", "los", "las", "y"}
        words = [w for w in address.sector.split() if w.lower() not in stop_words]
        abbr = "".join(w[0].upper() for w in words)[:5] if words else "XXX"
        prefix = f"VG-MAO-{abbr}-"
        
        cur.execute("SELECT COUNT(*) FROM addresses WHERE codigo LIKE ?", (prefix + "%",))
        count = cur.fetchone()[0]
        codigo = f"{prefix}{str(count + 1).zfill(5)}"
    else:
        codigo = address.codigo
    
    try:
        cur.execute("""
            INSERT INTO addresses 
            (codigo, provincia, municipio, sector, calle, numero, referencia, lat, lng, verificado, fuente, creado_por, notas)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            codigo, address.provincia, address.municipio, address.sector,
            address.calle, address.numero, address.referencia,
            address.lat, address.lng, address.verificado,
            address.fuente, address.creado_por, address.notas
        ))
        conn.commit()
        
        # Retornar el registro insertado
        cur.execute("SELECT * FROM addresses WHERE codigo = ?", (codigo,))
        conn.row_factory = sqlite3.Row
        result = dict(cur.fetchone())
        conn.close()
        return result
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=400, detail="El código ya existe")

@app.post("/addresses/campo")
def insert_campo(address: Address):
    """Alias para campo.html"""
    return insert_address(address)

@app.put("/addresses/{codigo}")
def update_address(codigo: str, address: AddressUpdate):
    """Actualiza dirección existente"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    # Verificar que existe
    cur.execute("SELECT * FROM addresses WHERE codigo = ?", (codigo,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Dirección no encontrada")
    
    # Construir UPDATE dinámico
    updates = []
    values = []
    
    for field, value in address.dict(exclude_unset=True).items():
        if field != "codigo" and value is not None:
            updates.append(f"{field} = ?")
            values.append(value)
    
    if not updates:
        conn.close()
        raise HTTPException(status_code=400, detail="No hay campos para actualizar")
    
    values.append(codigo)
    query = f"UPDATE addresses SET {', '.join(updates)} WHERE codigo = ?"
    
    cur.execute(query, values)
    conn.commit()
    
    # Retornar actualizado
    conn.row_factory = sqlite3.Row
    cur.execute("SELECT * FROM addresses WHERE codigo = ?", (codigo,))
    result = dict(cur.fetchone())
    conn.close()
    return result

@app.post("/ride/estimate")
def estimate_ride(data: dict):
    """Estima tarifa entre dos puntos"""
    import math
    
    pickup_codigo = data.get("pickup_codigo")
    dropoff_codigo = data.get("dropoff_codigo")
    
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    cur.execute("SELECT lat, lng FROM addresses WHERE codigo = ?", (pickup_codigo,))
    pickup = cur.fetchone()
    cur.execute("SELECT lat, lng FROM addresses WHERE codigo = ?", (dropoff_codigo,))
    dropoff = cur.fetchone()
    conn.close()
    
    if not pickup or not dropoff:
        raise HTTPException(status_code=404, detail="Dirección no encontrada")
    
    # Haversine
    R = 6371
    lat1, lng1 = math.radians(pickup[0]), math.radians(pickup[1])
    lat2, lng2 = math.radians(dropoff[0]), math.radians(dropoff[1])
    
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
    c = 2 * math.asin(math.sqrt(a))
    distance_km = R * c
    
    # Tarifa
    BASE_FARE = 75
    COST_PER_KM = 35
    AVG_SPEED = 25
    
    fare = BASE_FARE + (distance_km * COST_PER_KM)
    minutes = int((distance_km / AVG_SPEED) * 60)
    
    return {
        "distance_km": round(distance_km, 2),
        "estimated_minutes": max(3, minutes),
        "estimated_fare_rdp": round(fare, 2),
        "summary": f"{round(distance_km, 1)} km · ~{max(3, minutes)} min · RD${round(fare)}"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
