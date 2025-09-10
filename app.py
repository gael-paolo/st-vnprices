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

# Variable global para el filesystem
fs = None

# Lógica para inicializar el cliente de GCS
def initialize_gcs():
    """Inicializa el cliente de GCS con manejo de errores"""
    global fs
    try:
        # Verificar si existen los secrets
        if not hasattr(st, 'secrets') or not st.secrets:
            st.error("No se encontraron secrets configurados en Streamlit")
            return False
            
        # Obtener las credenciales del formato TOML de Streamlit
        if 'gcp_service_account' in st.secrets:
            service_account_info = dict(st.secrets["gcp_service_account"])
            
            # Asegurarse de que la private_key tenga el formato correcto
            if 'private_key' in service_account_info:
                service_account_info['private_key'] = service_account_info['private_key'].replace('\\n', '\n')
            
            # Crear credenciales
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            
            # Inicializar filesystem
            fs = gcsfs.GCSFileSystem(
                project=service_account_info['project_id'], 
                token=credentials
            )
            
            # Test simple de conexión
            try:
                fs.ls(GCS_BUCKET)
                st.success("✅ Conexión a GCS establecida correctamente")
                return True
            except Exception as test_error:
                st.error(f"❌ Error al conectar con GCS: {test_error}")
                return False
                
        else:
            st.error("No se encontró la sección 'gcp_service_account' en los secrets")
            return False
            
    except Exception as e:
        st.error(f"❌ Error al inicializar GCS: {e}")
        return False

# Inicializar GCS al cargar la aplicación
if 'gcs_initialized' not in st.session_state:
    if initialize_gcs():
        st.session_state.gcs_initialized = True
    else:
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
        return True
    except Exception as e:
        st.error(f"Error al guardar los datos JSON en GCS: {e}")
        return False

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
            
        return True

    except Exception as e:
        st.error(f"Error al guardar los datos en GCS: {e}")
        return False

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
    
    if save_json_file(USERS_FILE, users):
        return True, "Usuario creado exitosamente"
    else:
        return False, "Error al guardar el usuario"

def delete_user(username_to_delete):
    """Elimina un usuario del sistema"""
    users = load_json_file(USERS_FILE, {})
    
    if username_to_delete not in users:
        return False, "El usuario no existe."
    
    del users[username_to_delete]
    
    if save_json_file(USERS_FILE, users):
        return True, f"Usuario '{username_to_delete}' eliminado exitosamente."
    else:
        return False, "Error al eliminar el usuario"

def authenticate_user(username, password):
    """Autentica un usuario"""
    users = load_json_file(USERS_FILE, {})
    
    if username not in users:
        return False, "Usuario no encontrado"
    
    if not verify_password(password, users[username]["password"]):
        return False, "Contraseña incorrecta"
    
    users[username]["last_login"] = datetime.now().isoformat()
    if save_json_file(USERS_FILE, users):
        return True, "Autenticación exitosa"
    else:
        return False, "Error al actualizar el último login"

def get_user_role(username):
    """Obtiene el rol del usuario"""
    users = load_json_file(USERS_FILE, {})
    return users.get(username, {}).get("role", "asesor")

# Funciones de sesión
def create_session(username):
    """Crea una sesión para el usuario"""
    sessions = load_json_file(SESSIONS_FILE, {})
    session_id = hashlib.md5(f"{username}{datetime.now()}".encode()).hexdigest()
    
    sessions[session_id] = {
        "username": username,
        "created_at": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(hours=8)).isoformat()
    }
    
    if save_json_file(SESSIONS_FILE, sessions):
        return session_id
    else:
        return None

def validate_session(session_id):
    """Valida si una sesión es válida"""
    sessions = load_json_file(SESSIONS_FILE, {})
    
    if session_id not in sessions:
        return False, None
    
    session = sessions[session_id]
    expires_at = datetime.fromisoformat(session["expires_at"])
    
    if datetime.now() > expires_at:
        del sessions[session_id]
        save_json_file(SESSIONS_FILE, sessions)
        return False, None
    
    return True, session["username"]

def logout_session(session_id):
    """Cierra una sesión"""
    sessions = load_json_file(SESSIONS_FILE, {})
    if session_id in sessions:
        del sessions[session_id]
        save_json_file(SESSIONS_FILE, sessions)

# Inicializar datos por defecto
def initialize_default_data():
    """Inicializa datos de usuarios por defecto si no existen"""
    try:
        # Verificar si el archivo de usuarios existe
        if not fs.exists(USERS_FILE):
            st.info("🔧 Inicializando datos por defecto...")
            
            # Crear el directorio si no existe
            directory = os.path.dirname(USERS_FILE)
            if not fs.exists(directory):
                fs.makedirs(directory)
            
            # Crear usuarios por defecto
            users = {}
            
            default_users = [
                ("admin", "admin123", "admin"),
                ("gerencia_ventas", "ventas123", "gerencia_ventas"),
                ("gerencia_media", "media123", "gerencia_media"),
                ("asesor", "asesor123", "asesor")
            ]
            
            for username, password, role in default_users:
                users[username] = {
                    "password": hash_password(password),
                    "role": role,
                    "created_at": datetime.now().isoformat(),
                    "last_login": None,
                }
            
            # Guardar usuarios
            if save_json_file(USERS_FILE, users):
                st.success("✅ Datos por defecto inicializados correctamente")
            else:
                st.error("❌ Error al guardar usuarios por defecto")
                
    except Exception as e:
        st.error(f"❌ Error al inicializar datos por defecto: {e}")

# ... (las funciones de interfaz se mantienen igual como show_login_form, show_info_form, etc.)

def main():
    """Función principal de la aplicación"""
    try:
        # Verificar que GCS esté inicializado
        if 'gcs_initialized' not in st.session_state or not st.session_state.gcs_initialized:
            st.error("GCS no está inicializado. Recarga la página.")
            return
            
        # Inicializar datos por defecto
        initialize_default_data()
        
        if "session_id" in st.session_state:
            valid, username = validate_session(st.session_state.session_id)
            if not valid:
                for key in list(st.session_state.keys()):
                    if key != 'gcs_initialized':
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
                        if key != 'gcs_initialized':
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