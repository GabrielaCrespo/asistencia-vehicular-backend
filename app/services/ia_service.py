import json
import requests
from typing import Optional

import google.generativeai as genai

from .config import Config

_PROMPT = """Analiza este incidente vehicular y responde ÚNICAMENTE con un objeto JSON válido, sin texto adicional ni bloques de código markdown.

El JSON debe tener exactamente estos campos:
{{
  "transcripcion_audio": "transcripción literal del audio en español, o null si no hay audio",
  "clasificacion": "tipo específico de problema vehicular (ej: Batería descargada, Llanta pinchada, Choque frontal, Motor sobrecalentado)",
  "nivel_confianza": 0.85,
  "resultado_imagen": "descripción detallada de los daños visibles en la imagen, o null si no hay imagen",
  "prioridad": "alta",
  "resumen_automatico": "Resumen de 2-3 oraciones claras para el mecánico del taller.",
  "recomendaciones": "Acciones específicas y prácticas recomendadas para el técnico."
}}

Reglas de prioridad:
- "alta": peligro inmediato (accidente, frenos fallando, humo, incendio, pérdida de control)
- "normal": problema que impide conducir con seguridad pero sin riesgo vital
- "baja": falla menor, cosmética o eléctrica sin riesgo de seguridad

Información del incidente:
- Descripción del cliente: {descripcion}
{extra}

{instrucciones_medios}

Responde solo con el JSON. No agregues explicaciones fuera del JSON."""


def analizar_incidente(
    descripcion: str,
    tipo_problema: Optional[str],
    imagen_url: Optional[str],
    audio_url: Optional[str],
) -> dict:
    if not Config.GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY no está configurada en las variables de entorno")

    genai.configure(api_key=Config.GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    parts: list = []
    tiene_imagen = False
    tiene_audio = False

    # ── 1. Descargar y adjuntar audio ────────────────────────────────────────
    if audio_url:
        try:
            resp = requests.get(audio_url, timeout=20)
            resp.raise_for_status()
            audio_bytes = resp.content
            # Inferir mime type desde la URL
            mime_audio = "audio/mpeg"
            for ext, mime in [("wav", "audio/wav"), ("ogg", "audio/ogg"),
                               ("m4a", "audio/mp4"), ("webm", "audio/webm"),
                               ("flac", "audio/flac")]:
                if ext in audio_url.lower():
                    mime_audio = mime
                    break
            parts.append({"mime_type": mime_audio, "data": audio_bytes})
            tiene_audio = True
        except Exception:
            pass

    # ── 2. Descargar y adjuntar imagen ───────────────────────────────────────
    if imagen_url:
        try:
            resp = requests.get(imagen_url, timeout=20)
            resp.raise_for_status()
            img_bytes = resp.content
            mime_img = "image/jpeg"
            if "png" in imagen_url.lower():
                mime_img = "image/png"
            elif "webp" in imagen_url.lower():
                mime_img = "image/webp"
            elif "gif" in imagen_url.lower():
                mime_img = "image/gif"
            parts.append({"mime_type": mime_img, "data": img_bytes})
            tiene_imagen = True
        except Exception:
            pass

    # ── 3. Construir prompt textual ───────────────────────────────────────────
    extra_lines = []
    if tipo_problema:
        extra_lines.append(f"- Tipo de problema declarado: {tipo_problema}")
    extra = "\n".join(extra_lines)

    instrucciones: list[str] = []
    if tiene_audio:
        instrucciones.append("Se adjunta el audio del cliente describiendo el problema. Transcríbelo en el campo 'transcripcion_audio'.")
    else:
        instrucciones.append("No hay audio. Pon null en 'transcripcion_audio'.")
    if tiene_imagen:
        instrucciones.append("Se adjunta imagen del vehículo. Analiza los daños visibles en 'resultado_imagen'.")
    else:
        instrucciones.append("No hay imagen. Pon null en 'resultado_imagen'.")
    instrucciones_medios = " ".join(instrucciones)

    prompt_text = _PROMPT.format(
        descripcion=descripcion,
        extra=extra,
        instrucciones_medios=instrucciones_medios,
    )
    parts.append(prompt_text)

    # ── 4. Llamar a Gemini ────────────────────────────────────────────────────
    response = model.generate_content(parts)
    raw = response.text.strip()

    # Limpiar bloques de código markdown si el modelo los incluye
    if "```" in raw:
        for chunk in raw.split("```"):
            chunk = chunk.strip()
            if chunk.startswith("json"):
                chunk = chunk[4:].strip()
            if chunk.startswith("{"):
                raw = chunk
                break

    result = json.loads(raw)

    # ── 5. Agregar tipo_entrada ───────────────────────────────────────────────
    if tiene_imagen and tiene_audio:
        tipo_entrada = "multimodal"
    elif tiene_imagen:
        tipo_entrada = "imagen"
    elif tiene_audio:
        tipo_entrada = "audio"
    else:
        tipo_entrada = "texto"
    result["tipo_entrada"] = tipo_entrada

    return result
