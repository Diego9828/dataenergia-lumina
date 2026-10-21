"""Genera los capítulos de la Parte I del libro a partir de la sustentación de la Entrega 1.

Fuente única: docs/entrega1/sustentacion_entrega1.qmd (la misma que produce el Word).
Cada sección de nivel 1 de la sustentación va a un capítulo; los archivos se escriben junto a
la fuente (docs/entrega1/_e1_*.qmd) para que sus rutas a imágenes e includes sigan valiendo.
Quarto lo corre como pre-render; también se puede correr a mano:  python book/extraer_entrega1.py
"""
from __future__ import annotations

import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
FUENTE = RAIZ / "docs" / "entrega1" / "sustentacion_entrega1.qmd"
DESTINO = FUENTE.parent

# archivo, título del capítulo (None = sin encabezado, para incluir), secciones de la fuente
CAPITULOS: list[tuple[str, str | None, list[str]]] = [
    ("_e1_resumen.md", None, ["Resumen ejecutivo"]),
    ("_e1_arquitectura.qmd", "Arquitectura en cuatro capas", ["Arquitectura end-to-end"]),
    ("_e1_componentes.qmd", "Ingesta, almacenamiento y el porqué de cada componente",
     ["Definición de ingesta y almacenamiento", "Justificación del stack tecnológico"]),
    ("_e1_integracion.qmd", "Integración multifuente", ["Integración multifuente"]),
    ("_e1_escalabilidad.qmd", "Escalabilidad y performance", ["Escalabilidad y performance"]),
    ("_e1_economia.qmd", "Economía y calculadora de consumo",
     ["Justificación económica y optimización de costos"]),
    ("_e1_resiliencia.qmd", "Resiliencia, seguridad y alcance",
     ["Plan de recuperación y alta disponibilidad (DRP/HA)", "Seguridad y gobernanza",
      "Supuestos y fuera de alcance"]),
    ("_e1_glosario.qmd", "Glosario", ["Glosario"]),
    ("_e1_referencias.qmd", "Referencias", ["Referencias"]),
]
AVISO = "<!-- Generado por book/extraer_entrega1.py desde sustentacion_entrega1.qmd: no editar aquí. -->\n\n"


def secciones(texto: str) -> dict[str, list[str]]:
    """Separa el cuerpo por encabezados de nivel 1, ignorando los bloques de código."""
    cuerpo = re.sub(r"\A---\n.*?\n---\n", "", texto, flags=re.S)
    out: dict[str, list[str]] = {}
    actual, en_codigo = None, False
    for linea in cuerpo.splitlines():
        if linea.startswith("```"):
            en_codigo = not en_codigo
        if not en_codigo and linea.startswith("# "):
            actual = linea[2:].strip()
            out[actual] = []
        elif actual is not None:
            out[actual].append(linea)
    return out


def bajar_nivel(lineas: list[str]) -> list[str]:
    res, en_codigo = [], False
    for linea in lineas:
        if linea.startswith("```"):
            en_codigo = not en_codigo
        res.append("#" + linea if not en_codigo and re.match(r"#{1,5} ", linea) else linea)
    return res


def limpiar(lineas: list[str]) -> str:
    texto = "\n".join(lineas).strip() + "\n"
    return re.sub(r"\s*\(§ ?\d+\)", "", texto)        # referencias a secciones del Word


def main() -> None:
    sec = secciones(FUENTE.read_text(encoding="utf-8"))
    for archivo, titulo, nombres in CAPITULOS:
        faltan = [n for n in nombres if n not in sec]
        if faltan:
            raise SystemExit(f"Secciones no encontradas en {FUENTE.name}: {faltan}")
        if titulo is None:
            contenido = limpiar(sec[nombres[0]])
        elif len(nombres) == 1:
            contenido = f"# {titulo}\n\n" + limpiar(sec[nombres[0]])
        else:
            partes = [f"## {n}\n\n" + limpiar(bajar_nivel(sec[n])) for n in nombres]
            contenido = f"# {titulo}\n\n" + "\n".join(partes)
        (DESTINO / archivo).write_text(AVISO + contenido, encoding="utf-8", newline="\n")
    print(f"[libro] {len(CAPITULOS)} capítulos generados desde {FUENTE.name}")


if __name__ == "__main__":
    main()
