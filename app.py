import streamlit as st
import hashlib
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import time
from io import BytesIO
import shutil
import gcsfs
from google.cloud import storage
from google.oauth2 import service_account

# Configuración de la página
st.set_page_config(
    page_title="Sistema de Precios de Vehículos",
    page_icon="🚗",
    layout="wide"
)

# Configuración de GCP y rutas de archivo en el bucket
GCS_BUCKET = "bk-vn"
GCS_PATH = "nissan/prices"
USERS_FILE = f"gs://{GCS_BUCKET}/{GCS_PATH}/users.json"
SESSIONS_FILE = f"gs://{GCS_BUCKET}/{GCS_PATH}/sessions.json"
PRODUCTS_FILE = f"gs://{GCS_BUCKET}/{GCS_PATH}/products.csv"
PRODUCTS_HISTORICAL_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}/historical/"

# Lógica para inicializar el cliente de GCS
try:
    service_account_info = st.secrets["gcp_service_account"]
except KeyError:
    st.error("No se encontraron las credenciales de GCP. Asegúrate de que la sección `[gcp_service_account]` esté configurada en el archivo `.streamlit/secrets.toml`.")
    st.stop()

# Asegurar que la private_key tenga el formato correcto
if "private_key" in service_account_info:
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")

try:
    # Inicializar cliente de Google Cloud Storage (más confiable)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    storage_client = storage.Client(credentials=credentials)
    
    # También inicializar gcsfs para compatibilidad
    fs = gcsfs.GCSFileSystem(token=service_account_info)
    
except Exception as e:
    st.error(f"Error al inicializar el cliente de GCS: {e}")
    st.stop()

# Funciones de utilidad para archivos en GCS
def load_json_file(filename, default=None):
    """Carga un archivo JSON desde GCS, retorna default si no existe"""
    try:
        # Extraer bucket y path del filename gs://
        if filename.startswith("gs://"):
            bucket_name = filename.split("/")[2]
            blob_path = "/".join(filename.split("/")[3:])
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            if blob.exists():
                content = blob.download_as_text()
                return json.loads(content)
        else:
            if fs.exists(filename):
                with fs.open(filename, 'rb') as f:
                    return json.load(f)
    except Exception as e:
        st.error(f"Error al cargar el archivo JSON desde GCS: {e}")
    return default if default is not None else {}

def save_json_file(filename, data):
    """Guarda datos en un archivo JSON en GCS"""
    try:
        # Extraer bucket y path del filename gs://
        if filename.startswith("gs://"):
            bucket_name = filename.split("/")[2]
            blob_path = "/".join(filename.split("/")[3:])
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=2), content_type='application/json')
        else:
            with fs.open(filename, 'wb') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"Error al guardar los datos JSON en GCS: {e}")

def load_products(filename=PRODUCTS_FILE):
    """Carga la lista de productos desde un archivo CSV en GCS"""
    try:
        # Extraer bucket y path del filename gs://
        if filename.startswith("gs://"):
            bucket_name = filename.split("/")[2]
            blob_path = "/".join(filename.split("/")[3:])
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            if blob.exists():
                content = blob.download_as_bytes()
                return pd.read_csv(BytesIO(content))
        else:
            if fs.exists(filename):
                with fs.open(filename, 'rb') as f:
                    return pd.read_csv(f)
    except Exception as e:
        st.error(f"Error al cargar el archivo de precios desde GCS: {e}")
    return pd.DataFrame(columns=['Familia', 'Año', 'Precio_Nibol', 'Precio_Lista', 'Descuento', 'Precio_Final',
                                 'Dscto_Gerencia', 'Dsct_Seguro', 'Dscto_Impuesto', 'Bono', 'Precio_Gerencia',
                                 'Precio_BOB', 'USDT', 'USD_Ext', 'USD_Efect'])

def load_historical_products(historical_filename):
    """Carga un archivo de precios histórico desde GCS"""
    try:
        full_path = f"{PRODUCTS_HISTORICAL_PATH}{historical_filename}"
        
        # Extraer bucket y path
        if full_path.startswith("gs://"):
            bucket_name = full_path.split("/")[2]
            blob_path = "/".join(full_path.split("/")[3:])
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            if blob.exists():
                content = blob.download_as_bytes()
                return pd.read_csv(BytesIO(content))
        else:
            if fs.exists(full_path):
                with fs.open(full_path, 'rb') as f:
                    return pd.read_csv(f)
    except Exception as e:
        st.error(f"Error al cargar el archivo histórico de precios desde GCS: {e}")
    return pd.DataFrame(columns=['Familia', 'Año', 'Precio_Nibol', 'Precio_Lista', 'Descuento', 'Precio_Final',
                                 'Dscto_Gerencia', 'Dsct_Seguro', 'Dscto_Impuesto', 'Bono', 'Precio_Gerencia',
                                 'Precio_BOB', 'USDT', 'USD_Ext', 'USD_Efect'])

def save_products(df):
    """Guarda los datos en un archivo CSV en GCS con un nombre de fecha y actualiza el archivo principal"""
    try:
        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        historical_filename = f"{timestamp}_nissan_price_list.csv"
        
        # 1. Guardar archivo histórico
        historical_full_path = f"{PRODUCTS_HISTORICAL_PATH}{historical_filename}"
        
        if historical_full_path.startswith("gs://"):
            bucket_name = historical_full_path.split("/")[2]
            blob_path = "/".join(historical_full_path.split("/")[3:])
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
        else:
            with fs.open(historical_full_path, 'wb') as f:
                df.to_csv(f, index=False)
        
        # 2. Guardar archivo principal
        if PRODUCTS_FILE.startswith("gs://"):
            bucket_name = PRODUCTS_FILE.split("/")[2]
            blob_path = "/".join(PRODUCTS_FILE.split("/")[3:])
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
        else:
            with fs.open(PRODUCTS_FILE, 'wb') as f:
                df.to_csv(f, index=False)

    except Exception as e:
        st.error(f"Error al guardar los datos en GCS: {e}")

# ... (el resto de tus funciones se mantienen igual)

# Funciones de autenticación
def hash_password(password):
    """Hashea una contraseña usando SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed):
    """Verifica si la contraseña coincide con el hash"""
    return hash_password(password) == hashed

def create_user(username, password, role="asesor"):
    """Crea un nuevo usuario"""
    users = load_json_file(USERS_FILE, {})
    
    if username in users:
        return False, "El usuario ya existe"
    
    users[username] = {
        "password": hash_password(password),
        "role": role,
        "created_at": datetime.now().isoformat(),
        "last_login": None,
    }
    
    save_json_file(USERS_FILE, users)
    return True, "Usuario creado exitosamente"

# ... (continúa con el resto de tus funciones)

def main():
    """Función principal de la aplicación"""
    try:
        initialize_default_data()
        
        if "session_id" in st.session_state:
            valid, username = validate_session(st.session_state.session_id)
            if not valid:
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
            else:
                st.session_state.username = username
                st.session_state.user_role = get_user_role(username)
        
        if "session_id" not in st.session_state:
            st.title("🔐 Sistema de Precios de Vehículos")
            st.markdown("---")
            
            col1, col2 = st.columns(2)
            with col1:
                show_login_form()
            with col2:
                show_info_form()
                
            st.markdown("---")
        
        else:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.title(f"👋 Bienvenido, {st.session_state.username}")
                st.caption(f"Rol: {st.session_state.user_role}")
            with col2:
                if st.button("🚪 Cerrar Sesión", use_container_width=True):
                    logout_session(st.session_state.session_id)
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]
                    st.rerun()
            
            st.markdown("---")
            
            # Lógica para mostrar contenido basado en el rol
            user_role = st.session_state.user_role
            
            if user_role == "admin":
                admin_tab1, admin_tab2 = st.tabs(["🚗 Vehículos", "⚙️ Administración"])
                with admin_tab1:
                    show_products_dashboard(user_role)
                with admin_tab2:
                    show_admin_panel()
            elif user_role == "gerencia_ventas":
                ventas_tab1, ventas_tab2 = st.tabs(["🚗 Vehículos", "⚙️ Panel"])
                with ventas_tab1:
                    show_products_dashboard(user_role)
                with ventas_tab2:
                    show_admin_panel_ventas()
            elif user_role == "gerencia_media":
                media_tab1, media_tab2 = st.tabs(["🚗 Vehículos", "⚙️ Panel"])
                with media_tab1:
                    show_products_dashboard(user_role)
                with media_tab2:
                    show_admin_panel_media()
            elif user_role == "asesor":
                show_products_dashboard(user_role)
                
    except Exception as e:
        st.error(f"Error crítico en la aplicación: {e}")
        st.error("Por favor, verifica tu conexión y configuración de GCP.")

if __name__ == "__main__":
    main()