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
from google.oauth2 import service_account

# Configuración de la página
st.set_page_config(
    page_title="Sistema de Precios de Vehículos",
    page_icon="🚗",
    layout="wide"
)

# Configuración de GCP y rutas de archivo en el bucket
GCS_BUCKET = "bk_vn"
GCS_PATH = "nissan/prices"
USERS_FILE = f"{GCS_BUCKET}/{GCS_PATH}/users.json"
SESSIONS_FILE = f"{GCS_BUCKET}/{GCS_PATH}/sessions.json"
PRODUCTS_FILE = f"{GCS_BUCKET}/{GCS_PATH}/products.csv"
PRODUCTS_HISTORICAL_PATH = f"{GCS_BUCKET}/{GCS_PATH}/historical/"

# Lógica para inicializar el cliente de GCS
try:
    # Obtener credenciales de los secrets
    if 'gcp_service_account' in st.secrets:
        service_account_info = dict(st.secrets["gcp_service_account"])
    else:
        # Intentar formato alternativo
        service_account_info = {
            "type": st.secrets.get("type"),
            "project_id": st.secrets.get("project_id"),
            "private_key_id": st.secrets.get("private_key_id"),
            "private_key": st.secrets.get("private_key").replace("\\n", "\n"),
            "client_email": st.secrets.get("client_email"),
            "client_id": st.secrets.get("client_id"),
            "auth_uri": st.secrets.get("auth_uri"),
            "token_uri": st.secrets.get("token_uri"),
            "auth_provider_x509_cert_url": st.secrets.get("auth_provider_x509_cert_url"),
            "client_x509_cert_url": st.secrets.get("client_x509_cert_url")
        }
    
    # Crear credenciales
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    
    # Inicializar filesystem
    fs = gcsfs.GCSFileSystem(project=service_account_info['project_id'], token=credentials)
    
except Exception as e:
    st.error(f"Error al inicializar el cliente de GCS: {e}")
    st.error("Asegúrate de que las credenciales de GCP estén configuradas correctamente en Streamlit Secrets")
    st.stop()

# Funciones de utilidad para archivos en GCS
def load_json_file(filename, default=None):
    """Carga un archivo JSON desde GCS, retorna default si no existe"""
    try:
        if fs.exists(filename):
            with fs.open(filename, 'r') as f:
                return json.load(f)
    except Exception as e:
        st.error(f"Error al cargar el archivo JSON desde GCS: {e}")
    return default if default is not None else {}

def save_json_file(filename, data):
    """Guarda datos en un archivo JSON en GCS"""
    try:
        with fs.open(filename, 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"Error al guardar los datos JSON en GCS: {e}")

def load_products(filename=PRODUCTS_FILE):
    """Carga la lista de productos desde un archivo CSV en GCS"""
    try:
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
        
        # 1. Guarda el DataFrame en el archivo histórico
        with fs.open(f"{PRODUCTS_HISTORICAL_PATH}{historical_filename}", 'wb') as f:
            df.to_csv(f, index=False)
        
        # 2. Guarda el mismo DataFrame en el archivo principal (products.csv)
        with fs.open(PRODUCTS_FILE, 'wb') as f:
            df.to_csv(f, index=False)

    except Exception as e:
        st.error(f"Error al guardar los datos en GCS: {e}")

# ... (el resto de las funciones se mantienen igual)

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
        st.error("Por favor, recarga la página o contacta al administrador")

if __name__ == "__main__":
    main()