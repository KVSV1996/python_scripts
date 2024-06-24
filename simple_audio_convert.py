from pydub import AudioSegment
import os

def convert_audio(input_folder, output_folder):
    # Проверяем, существует ли выходная папка, если нет - создаем ее
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Проходим по всем файлам в входной папке
    for filename in os.listdir(input_folder):
        if filename.endswith(('.mp3', '.ogg', '.flv', '.wav', '.m4a', '.aac')):  # Добавьте другие поддерживаемые форматы по необходимости
            # Определяем путь к входному и выходному файлу
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, os.path.splitext(filename)[0] + '.wav')
            
            # Загружаем аудиофайл
            audio = AudioSegment.from_file(input_path)
            
            # Преобразуем аудиофайл
            audio = audio.set_frame_rate(8000).set_channels(1).set_sample_width(2)
            
            # Сохраняем аудиофайл в формате WAV
            audio.export(output_path, format='wav')
            print(f"Конвертировано: {output_path}")

# Укажите путь к вашей входной и выходной папкам
input_folder = '/home/VK/prizvicha'
output_folder = '/home/VK/prizvichanew/'

convert_audio(input_folder, output_folder)

