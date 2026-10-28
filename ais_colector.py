"""
Colector AIS · Río de la Plata → Rosario
----------------------------------------
Escucha durante unos minutos los datos AIS gratuitos de aisstream.io en la
zona Río de la Plata – Paraná – Gran Rosario y guarda el último estado de cada
buque en datos/ais.json. El sistema de Line Up TPR (Google Apps Script) lee ese
archivo cada 30 minutos.

Variables de entorno:
  AISSTREAM_KEY  (obligatoria) API key de aisstream.io
  MINUTOS        (opcional, por defecto 10) minutos de escucha
"""
import asyncio, json, os, sys, time
from datetime import datetime, timedelta, timezone

URL = "wss://stream.aisstream.io/v0/stream"
ARCHIVO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datos", "ais.json")
CAJA = [[-35.8, -61.0], [-32.4, -54.8]]          # rectángulo pedido a aisstream
TIPOS = ["PositionReport", "ShipStaticData"]       # solo buques Clase A (mercantes)
CONSERVAR_HORAS = 72                               # buques no vistos en 72 h se descartan

# Zonas del río (lat_min, lat_max, lon_min, lon_max). Orden: de la más específica a la más general.
ZONAS = [
    ("TPR",                          -32.975, -32.925, -60.650, -60.605),
    ("Rosario",                      -33.100, -32.850, -60.750, -60.550),
    ("San Lorenzo / Timbúes",        -32.850, -32.450, -60.850, -60.600),
    ("Villa Const. / San Nicolás",   -33.450, -33.100, -60.450, -60.100),
    ("Paraná inferior",              -34.500, -33.300, -60.300, -58.300),
    ("Río de la Plata",              -35.800, -34.300, -58.500, -54.800),
]
ESTADOS = {0: "Navegando", 1: "Fondeado", 2: "Sin gobierno", 3: "Maniobra restringida",
           5: "Amarrado", 6: "Varado", 15: "Sin dato"}


def zona_de(lat, lon):
    for nombre, la1, la2, lo1, lo2 in ZONAS:
        if la1 <= lat <= la2 and lo1 <= lon <= lo2:
            return nombre
    return "Otra"


def hora_iso(t):
    # "2026-10-28 19:17:14.123 +0000 UTC" -> "2026-10-28T19:17:14Z"
    t = str(t or "")[:19].replace(" ", "T")
    return t + "Z" if len(t) == 19 else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Registro:
    def __init__(self, previos=None):
        self.buques = {str(b["mmsi"]): b for b in (previos or [])}
        self.mensajes = 0

    def procesar(self, raw):
        m = json.loads(raw)
        if "error" in m:
            raise RuntimeError("aisstream: " + str(m["error"]))
        tipo = m.get("MessageType")
        meta = m.get("MetaData", {})
        cuerpo = m.get("Message", {}).get(tipo, {})
        mmsi = str(meta.get("MMSI") or cuerpo.get("UserID") or "")
        if not mmsi:
            return
        self.mensajes += 1
        b = self.buques.setdefault(mmsi, {"mmsi": mmsi, "nombre": "", "imo": "", "destino": "", "eta_ais": "",
                                          "lat": None, "lon": None, "velocidad": None, "estado": "", "zona": "",
                                          "visto_utc": ""})
        nombre = (meta.get("ShipName") or "").strip()
        if nombre:
            b["nombre"] = nombre
        lat, lon = meta.get("latitude"), meta.get("longitude")
        if tipo == "PositionReport":
            lat, lon = cuerpo.get("Latitude", lat), cuerpo.get("Longitude", lon)
            b["velocidad"] = cuerpo.get("Sog")
            b["estado"] = ESTADOS.get(cuerpo.get("NavigationalStatus"), str(cuerpo.get("NavigationalStatus", "")))
        elif tipo == "ShipStaticData":
            b["nombre"] = (cuerpo.get("Name") or b["nombre"]).strip()
            b["imo"] = str(cuerpo.get("ImoNumber") or b["imo"] or "")
            b["destino"] = (cuerpo.get("Destination") or "").strip()
            e = cuerpo.get("Eta") or {}
            if e.get("Month") and e.get("Day"):
                b["eta_ais"] = "%02d/%02d %02d:%02d UTC" % (e["Day"], e["Month"], e.get("Hour", 0) % 24, e.get("Minute", 0) % 60)
        if lat is not None and lon is not None and abs(lat) <= 90 and abs(lon) <= 180:
            b["lat"], b["lon"] = round(lat, 5), round(lon, 5)
            b["zona"] = zona_de(lat, lon)
            b["visto_utc"] = hora_iso(meta.get("time_utc"))

    def salida(self):
        limite = datetime.now(timezone.utc) - timedelta(hours=CONSERVAR_HORAS)
        vivos = []
        for b in self.buques.values():
            try:
                visto = datetime.strptime(b["visto_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except (ValueError, KeyError):
                continue
            if visto >= limite and b.get("lat") is not None:
                vivos.append(b)
        vivos.sort(key=lambda b: (b["zona"], b["nombre"]))
        return {"generado_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "mensajes_ultima_corrida": self.mensajes, "buques": vivos}


async def escuchar(reg, minutos, clave):
    import websockets
    fin = time.time() + minutos * 60
    intentos = 0
    while time.time() < fin:
        try:
            async with websockets.connect(URL, ping_interval=20, max_size=None) as ws:
                await ws.send(json.dumps({"APIKey": clave, "BoundingBoxes": [CAJA], "FilterMessageTypes": TIPOS}))
                while time.time() < fin:
                    try:
                        reg.procesar(await asyncio.wait_for(ws.recv(), timeout=5))
                    except asyncio.TimeoutError:
                        pass
        except RuntimeError:
            raise
        except Exception as e:  # corte de conexión: reintentar
            intentos += 1
            espera = min(30, 2 ** intentos)
            print(f"Conexión cortada ({type(e).__name__}: {e}); reintento en {espera} s", flush=True)
            await asyncio.sleep(espera)


def main():
    clave = os.environ.get("AISSTREAM_KEY", "").strip()
    if not clave:
        sys.exit("Falta el secreto AISSTREAM_KEY")
    minutos = float(os.environ.get("MINUTOS", "10"))
    previos = []
    if os.path.exists(ARCHIVO):
        try:
            previos = json.load(open(ARCHIVO, encoding="utf-8")).get("buques", [])
        except Exception:
            previos = []
    reg = Registro(previos)
    print(f"Escuchando {minutos} min…", flush=True)
    asyncio.run(escuchar(reg, minutos, clave))
    datos = reg.salida()
    os.makedirs(os.path.dirname(ARCHIVO), exist_ok=True)
    with open(ARCHIVO, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=1)
    print(f"{reg.mensajes} mensajes · {len(datos['buques'])} buques guardados", flush=True)


if __name__ == "__main__":
    main()
