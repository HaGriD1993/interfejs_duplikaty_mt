import io
import uuid
import requests
import os
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from azure.core.exceptions import ResourceExistsError
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# ===== Konfiguracja =====:
load_dotenv()
AZURE_CONNECTION_STRING = os.getenv('AZURE_CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
AZURE_FUNCTION_URL = os.getenv('AZURE_FUNCTION_URL')

# ===== Funkcje pomocnicze =====:
def check_connection():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        container_client.get_container_properties()
        return True
    except AzureError:
        return False

def get_container_stats_and_data(container_client):
    total_size_bytes = 0
    blob_count = 0
    file_data = []
    for blob in container_client.list_blobs():
        size_mb = blob.size / (1024 * 1024)
        total_size_bytes += blob.size
        blob_count += 1
        file_data.append({"Nazwa pliku": blob.name, "Rozmiar (MB)": round(size_mb, 2)})
    return blob_count, total_size_bytes / (1024 * 1024), file_data

def generate_pdf(data_list):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 50, "Raport plików w kontenerze Azure")

    c.setFont("Helvetica", 11)
    y = height - 90
    c.drawString(50, y, "Nazwa pliku")
    c.drawString(400, y, "Rozmiar (MB)")
    y -= 15

    for item in data_list:
        if y < 50:
            c.showPage()
            y = height - 50
        c.drawString(50, y, str(item["Nazwa pliku"]))
        c.drawString(400, y, f"{item['Rozmiar (MB)']:.2f}")
        y -= 15

    c.save()
    buffer.seek(0)
    return buffer

def generate_blob_name(original_filename):
    prefix = str(uuid.uuid4()).replace("-", "")[:6]
    return f"{prefix}-{original_filename}"

def trigger_azure_function():
    try:
        # Inicjalizacja klienta kontenera
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # Liczba plików Przed analizą
        before_count = sum(1 for _ in container_client.list_blobs())

        # Wywołanie funkcji Azure
        response = requests.get(AZURE_FUNCTION_URL)  # lub .post() jeśli Twoja funkcja wymaga POST

        if response.status_code != 200:
            return None, f"Błąd podczas wywołania funkcji Azure: {response.status_code}"

        # Liczba plików Po analizie
        after_count = sum(1 for _ in container_client.list_blobs())

        return {
            "before": before_count,
            "after": after_count,
            "difference": before_count - after_count
        }, None

    except Exception as e:
        return None, str(e)
    

# ===== Nagłówek z autorem =====:
st.markdown("### **Praca inżynierska:** System rozpoznawania i usuwania duplikatów plików przechowywanych w chmurze.")
st.markdown("###### **Autor:** Michał Tkacz")
st.markdown("###### **Numer indeksu:**  29395 ")
st.markdown("-------------------------------------------------------------------------------------")

# ===== Aplikacja Streamlit =====:
st.title("☁️📦Uploader plików do Azure Blob Storage:")
st.markdown("""Prosta i wydajna aplikacja do przesyłania plików do Azure Blob Storage. 
            Przeciągnij i upuść pliki lub wybierz je ręcznie, a aplikacja automatycznie przekaże je do wskazanego kontenera w chmurze Microsoft Azure.""")
    # Konfiguracja strony
st.set_page_config(
    page_title="Azure Uploader Michał Tkacz",
    page_icon="☁️"
)
st.markdown("-------------------------------------------------------------------------------------")

# ===== Sprawdzanie połączenia: =====:
st.markdown("### 🔍 Status połączenia z Azure Blob Storage:")
if check_connection():
    st.success("🟢 Połączenie z Azure Blob Storage działa.")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
else:
    st.error("🔴 Brak połączenia z Azure Blob Storage.")
    st.stop()
st.markdown("-------------------------------------------------------------------------------------")

st.markdown("### 📤 Przesyłanie plików:")
st.markdown("""
W tej sekcji możesz wybrać i przesłać pliki do kontenera Azure Blob Storage.
- Obsługiwane jest przesyłanie wielu plików jednocześnie.
- Każdy plik otrzyma unikalną nazwę, aby uniknąć nadpisania.
- Po przesłaniu otrzymasz potwierdzenie dla każdego pliku.
""")
uploaded_files = st.file_uploader("Wybierz pliki do analizy:",accept_multiple_files=True)

if st.button("Wyślij do chmury"):
    if not uploaded_files:
        st.warning("Najpierw wybierz pliki.")
    else:
        for file in uploaded_files:
            success = False
            attempts = 0
            max_attempts = 5

            while not success and attempts < max_attempts:
                blob_name = generate_blob_name(file.name)
                blob_client = container_client.get_blob_client(blob_name)

                try:
                    blob_client.upload_blob(file, overwrite=False)
                    st.success(f"✅ Plik '{file.name}' wysłany jako '{blob_name}'.")
                    success = True
                except ResourceExistsError:
                    attempts += 1
                    st.warning(f"⚠️ Nazwa '{blob_name}' już istnieje. Próba {attempts} z {max_attempts}...")

            if not success:
                st.error(f"❌ Nie udało się przesłać pliku '{file.name}' po {max_attempts} próbach.")
st.markdown("-------------------------------------------------------------------------------------")

# ===== Statystyki i wykres =====:
st.header("📊 Statystyki kontenera:")

blob_count, total_size_mb, file_data = get_container_stats_and_data(container_client)

st.write(f"**Liczba plików**: {blob_count}")
st.write(f"**Łączny rozmiar**: {total_size_mb:.2f} MB")

plt.style.use('dark_background')
fig, ax = plt.subplots()
ax.bar(["Liczba plików", "Rozmiar (MB)"], [blob_count, total_size_mb], color=["blue", "green"],width=0.5)
ax.set_ylabel("Wartość")
ax.set_title("Statystyki kontenera Azure Blob Storage")
st.pyplot(fig)
st.markdown("-------------------------------------------------------------------------------------")

# ===== Przeglądarka plików w kontenerze =====:
st.header("🗂️ Lista plików w kontenerze")

with st.expander("Kliknij, aby wyświetlić listę plików:"):
    if file_data:
        df_files = pd.DataFrame(file_data)
        st.dataframe(df_files, use_container_width=True)
    else:
        st.info("Brak plików w kontenerze.")
st.markdown("-------------------------------------------------------------------------------------")

# ======= Wywołanie Azure Function =======:
st.header("⚙️🗑️ Analiza i usuwanie duplikatów:")
st.markdown("Kliknij przycisk poniżej, aby uruchomić Azure Function i przeprowadzić analizę oraz usunąć duplikaty z kontenera.")

if st.button("Dokonaj analizy i usuń duplikaty"):
    result, error = trigger_azure_function()

    if error:
        st.error(f"❌ Wystąpił błąd: {error}")
    else:
        st.success("✅ Analiza zakończona pomyślnie.")
        st.info(f"📂 Plików przed analizą: **{result['before']}**")
        st.info(f"📁 Plików po analizie: **{result['after']}**")
        st.info(f"🗑️ Usunięto duplikatów: **{result['difference']}**")
st.markdown("-------------------------------------------------------------------------------------")

# ===== Pobieranie CSV =====: 
st.header("📥 Generowanie i pobieranie raportów z chmury")
st.markdown("Wygeneruj i pobierz raport CSV lub PDF zawierający listę plików zapisanych w Azure Blob Storage.")
df = pd.DataFrame(file_data)
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)

st.download_button(
    label="📥 Pobierz raport CSV",
    data=csv_buffer.getvalue(),
    file_name="raport_blob.csv",
    mime="text/csv"
)

# ===== Pobieranie PDF =====:
pdf_buffer = generate_pdf(file_data)

st.download_button(
    label="📄 Pobierz raport PDF",
    data=pdf_buffer,
    file_name="raport_blob.pdf",
    mime="application/pdf"
)
st.markdown("-------------------------------------------------------------------------------------")