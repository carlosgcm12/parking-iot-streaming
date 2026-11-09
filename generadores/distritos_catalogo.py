"""
Catalogo de distritos de zona azul (SER) de Malaga capital (Fuente B).

Cada distrito tiene un punto base real (una calle/zona conocida) sobre el
que se dispersan los sensores simulados con un pequeno jitter aleatorio,
para que las coordenadas tengan sentido geografico real sin necesitar la
ubicacion exacta de cada plaza (que no es un dato publico disponible).
"""

DISTRITOS = [
    {"nombre": "Centro", "calle_base": "C/ Larios", "lat": 36.7212, "lon": -4.4200, "n_sensores": 90},
    {"nombre": "Huelin", "calle_base": "Paseo Maritimo Antonio Machado", "lat": 36.7089, "lon": -4.4360, "n_sensores": 80},
    {"nombre": "Cruz de Humilladero", "calle_base": "C/ Antonio Martelo", "lat": 36.7059, "lon": -4.4498, "n_sensores": 60},
    {"nombre": "Avenida de la Aurora", "calle_base": "Av. de la Aurora", "lat": 36.7136, "lon": -4.4380, "n_sensores": 40},
    {"nombre": "Teatinos-Universidad", "calle_base": "Bulevar Louis Pasteur", "lat": 36.7178, "lon": -4.4785, "n_sensores": 50},
    {"nombre": "El Palo", "calle_base": "Av. Salvador Allende", "lat": 36.7280, "lon": -4.3745, "n_sensores": 30},
    {"nombre": "Carretera de Cadiz", "calle_base": "Av. Velazquez", "lat": 36.7057, "lon": -4.4552, "n_sensores": 50},
]

# Jitter maximo en grados (~0.003 grados equivale aprox a 300m, suficiente
# para dispersar sensores dentro del mismo distrito sin salirse de el).
JITTER_MAX = 0.004

TOTAL_SENSORES = sum(d["n_sensores"] for d in DISTRITOS)  # 400
