import streamlit as st
import whisper
import torch
import numpy as np
import speech_recognition as sr
from queue import Queue
from datetime import datetime, timedelta
import os
from time import sleep

# Configuración inicial
device = "cuda" if torch.cuda.is_available() else "cpu"

# Cargar modelo Whisper
@st.cache_resource
def load_model(model_size="medium"):
    return whisper.load_model(model_size, device=device)

# Inicialización de elementos principales
data_queue = Queue()
transcription = ['']

# Función para la transcripción en tiempo real
def process_audio(model, data_queue, transcription, record_timeout=2, phrase_timeout=3):
    """
    Procesa el audio en tiempo real desde la cola y realiza transcripciones utilizando Whisper.
    """
    phrase_time = None
    now = datetime.utcnow()
    
    if not data_queue.empty():
        phrase_complete = False

        # Si ha pasado suficiente tiempo desde la última grabación, se completa la frase.
        if phrase_time and now - phrase_time > timedelta(seconds=phrase_timeout):
            phrase_complete = True
        
        # Actualizar el tiempo de la última grabación
        phrase_time = now

        # Combinar datos de audio en la cola
        audio_data = b''.join(data_queue.queue)
        data_queue.queue.clear()

        # Convertir datos de audio a un formato utilizable por Whisper
        audio_np = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32) / 32768.0

        # Realizar la transcripción
        result = model.transcribe(audio_np, fp16=torch.cuda.is_available())
        text = result['text'].strip()

        # Actualizar la transcripción dependiendo de si se completa o no la frase
        if phrase_complete:
            transcription.append(text)
        else:
            transcription[-1] = text

    return transcription

# Streamlit Interface
st.title("Transcripción de Audio en Tiempo Real con Whisper")
st.info("Este sistema usa Whisper y optimización con GPU para transcribir audio en tiempo real.")

# Configuración del modelo
model_size = st.selectbox("Selecciona el tamaño del modelo Whisper:", ["tiny", "base", "small", "medium", "large"])
model = load_model(model_size)

# Inicialización del micrófono
recognizer = sr.Recognizer()
recognizer.energy_threshold = 1000
recognizer.dynamic_energy_threshold = False

# Micrófono
if st.button("Iniciar Transcripción"):
    try:
        mic = sr.Microphone(sample_rate=16000)
        with mic as source:
            recognizer.adjust_for_ambient_noise(source)
        
        # Función callback para recibir audio
        def record_callback(_, audio: sr.AudioData):
            data = audio.get_raw_data()
            data_queue.put(data)

        # Iniciar la grabación en segundo plano
        recognizer.listen_in_background(mic, record_callback, phrase_time_limit=2)
        st.success("Grabación iniciada. Habla ahora.")

        # Inicialización del contador de claves únicas
        unique_key = 0

        # Mantener la transcripción en tiempo real
        while True:
            transcription = process_audio(model, data_queue, transcription)

            # Mostrar transcripción en tiempo real con un key único
            unique_key += 1
            st.text_area(
                "Texto Transcrito:",
                "\n".join(transcription),
                height=300,
                key=f"transcription_area_{unique_key}"
            )

            # Dormir para evitar saturación del procesador
            sleep(0.25)
    except KeyboardInterrupt:
        st.warning("Transcripción detenida.")
    except Exception as e:
        st.error(f"Error al iniciar el micrófono: {e}")
