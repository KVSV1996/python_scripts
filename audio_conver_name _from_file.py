import pandas as pd
import os
from pydub import AudioSegment

# Загрузите таблицу с именами файлов с указанием правильного разделителя
file_path = '/home/Vladislav.Kozyrev/testsur.csv'
df = pd.read_csv(file_path, delimiter=';', header=None, names=['Old Name', 'New Name'])

# Проверьте заголовки таблицы
print("Заголовки таблицы:", df.columns)

# Конвертируем значения колонок в строки
df['Old Name'] = df['Old Name'].astype(str) + '.m4a'
df['New Name'] = df['New Name'].astype(str) + '.wav'

# Путь к папке с файлами (замените 'path/to/your/files' на реальный путь)
folder_path = '/home/Vladislav.Kozyrev/sur/'

# Папка для сохранения конвертированных файлов
output_folder = '/home/Vladislav.Kozyrev/newsur/'
os.makedirs(output_folder, exist_ok=True)

# Итерация по строкам таблицы, переименование и конвертация файлов
for index, row in df.iterrows():
    old_name = row['Old Name']  # Колонка с текущими именами файлов
    new_name = row['New Name']  # Колонка с новыми именами файлов

    old_file_path = os.path.join(folder_path, old_name)
    new_file_path = os.path.join(output_folder, new_name)

    # Переименование и конвертация файла
    try:
        # Загрузка аудиофайла
        audio = AudioSegment.from_file(old_file_path)

        # Конвертация в моно, 8000 Гц, 16-бит
        audio = audio.set_frame_rate(8000).set_channels(1).set_sample_width(2)

        # Сохранение конвертированного файла в формате WAV
        audio.export(new_file_path, format='wav')

        print(f"File {old_name} converted and renamed to {new_name}")
    except FileNotFoundError:
        print(f"File {old_name} not found")
    except Exception as e:
        print(f"Error processing file {old_name}: {e}")

print("Conversion and renaming completed.")
