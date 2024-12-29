import whisper
import torch
import numpy as np
import speech_recognition as sr
from queue import Queue
from datetime import datetime, timedelta

class TranscriptManager:
    def __init__(self, model_size="medium", record_timeout=2, phrase_timeout=3):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = self.load_model(model_size)
        self.data_queue = Queue()
        self.transcription = ['']
        self.record_timeout = record_timeout
        self.phrase_timeout = phrase_timeout
        self.recognizer = sr.Recognizer()
        self.recognizer.energy_threshold = 1000
        self.recognizer.dynamic_energy_threshold = False
        self.listening = False

    def load_model(self, model_size):
        return whisper.load_model(model_size, device=self.device)

    def process_audio(self):
        phrase_time = None
        now = datetime.utcnow()
        
        if not self.data_queue.empty():
            phrase_complete = False

            if phrase_time and now - phrase_time > timedelta(seconds=self.phrase_timeout):
                phrase_complete = True
            
            phrase_time = now

            audio_data = b''.join(self.data_queue.queue)
            self.data_queue.queue.clear()

            audio_np = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32) / 32768.0

            result = self.model.transcribe(audio_np, fp16=torch.cuda.is_available())
            text = result['text'].strip()

            if phrase_complete:
                self.transcription.append(text)
            else:
                self.transcription[-1] = text

        return self.transcription

    def start_listening(self):
        self.listening = True
        mic = sr.Microphone(sample_rate=16000)
        with mic as source:
            self.recognizer.adjust_for_ambient_noise(source)
        
        def record_callback(_, audio: sr.AudioData):
            if self.listening:
                data = audio.get_raw_data()
                self.data_queue.put(data)

        self.recognizer.listen_in_background(mic, record_callback, phrase_time_limit=2)
        print("Grabación iniciada. Mantén pulsado el botón para hablar.")

    def stop_listening(self):
        self.listening = False
        print("Grabación detenida.")

    def get_transcription(self):
        self.transcription = self.process_audio()
        return "\n".join(self.transcription)
