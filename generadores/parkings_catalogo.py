"""
Catalogo de parkings publicos gestionados de Malaga capital (Fuente A).

Datos reales de referencia (nombre, zona, tarifa/hora, ubicacion aproximada,
capacidad aproximada). Se usan tal cual para dar variabilidad real de tarifa
segun ubicacion, tal como se decidio en el diseno del proyecto.
"""

PARKINGS = [
    {
        "parking_id": "PK-001",
        "nombre": "Parking La Marina",
        "zona": "Centro",
        "plazas_totales": 400,
        "tarifa_hora": 2.35,
        "lat": 36.7196,
        "lon": -4.4200,
    },
    {
        "parking_id": "PK-002",
        "nombre": "Parking Alcazaba",
        "zona": "Centro",
        "plazas_totales": 300,
        "tarifa_hora": 2.00,
        "lat": 36.7215,
        "lon": -4.4167,
    },
    {
        "parking_id": "PK-003",
        "nombre": "Parking Granados (Uncibay)",
        "zona": "Centro",
        "plazas_totales": 220,
        "tarifa_hora": 2.40,
        "lat": 36.7212,
        "lon": -4.4198,
    },
    {
        "parking_id": "PK-004",
        "nombre": "Parking San Juan de la Cruz",
        "zona": "Centro",
        "plazas_totales": 350,
        "tarifa_hora": 1.90,
        "lat": 36.7205,
        "lon": -4.4145,
    },
    {
        "parking_id": "PK-005",
        "nombre": "Parking Cervantes",
        "zona": "Centro",
        "plazas_totales": 180,
        "tarifa_hora": 1.85,
        "lat": 36.7238,
        "lon": -4.4189,
    },
    {
        "parking_id": "PK-006",
        "nombre": "Parking Indigo Mena",
        "zona": "Centro",
        "plazas_totales": 260,
        "tarifa_hora": 1.58,
        "lat": 36.7229,
        "lon": -4.4213,
    },
    {
        "parking_id": "PK-007",
        "nombre": "Parking Cruz de Humilladero",
        "zona": "Cruz de Humilladero",
        "plazas_totales": 150,
        "tarifa_hora": 1.50,
        "lat": 36.7059,
        "lon": -4.4498,
    },
    {
        "parking_id": "PK-008",
        "nombre": "Parking El Palo",
        "zona": "El Palo",
        "plazas_totales": 120,
        "tarifa_hora": 1.50,
        "lat": 36.7280,
        "lon": -4.3745,
    },
    {
        "parking_id": "PK-009",
        "nombre": "Parking Carlos Haya",
        "zona": "Carretera de Cadiz",
        "plazas_totales": 200,
        "tarifa_hora": 1.50,
        "lat": 36.7104,
        "lon": -4.4552,
    },
    {
        "parking_id": "PK-010",
        "nombre": "Parking Estacion Maria Zambrano",
        "zona": "Estacion",
        "plazas_totales": 500,
        "tarifa_hora": 1.80,
        "lat": 36.7127,
        "lon": -4.4324,
    },
]


def get_parking(parking_id: str) -> dict:
    for p in PARKINGS:
        if p["parking_id"] == parking_id:
            return p
    raise KeyError(f"Parking no encontrado: {parking_id}")
