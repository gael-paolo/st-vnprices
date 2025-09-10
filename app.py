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

def delete_user(username_to_delete):
    """Elimina un usuario del sistema"""
    users = load_json_file(USERS_FILE, {})
    
    if username_to_delete not in users:
        return False, "El usuario no existe."
    
    del users[username_to_delete]
    
    save_json_file(USERS_FILE, users)
    return True, f"Usuario '{username_to_delete}' eliminado exitosamente."

def authenticate_user(username, password):
    """Autentica un usuario"""
    users = load_json_file(USERS_FILE, {})
    
    if username not in users:
        return False, "Usuario no encontrado"
    
    if not verify_password(password, users[username]["password"]):
        return False, "Contraseña incorrecta"
    
    users[username]["last_login"] = datetime.now().isoformat()
    save_json_file(USERS_FILE, users)
    
    return True, "Autenticación exitosa"

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
    
    save_json_file(SESSIONS_FILE, sessions)
    return session_id

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
        if not fs.exists(USERS_FILE):
            create_user("admin", "admin123", "admin")
            create_user("gerencia_ventas", "ventas123", "gerencia_ventas")
            create_user("gerencia_media", "media123", "gerencia_media")
            create_user("asesor", "asesor123", "asesor")
    except Exception as e:
        st.error(f"Error al inicializar datos por defecto: {e}")

# Funciones de interfaz
def show_login_form():
    """Muestra el formulario de login"""
    st.markdown("## 🔐 Iniciar Sesión")
    
    with st.form("login_form"):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        submit = st.form_submit_button("Iniciar Sesión", use_container_width=True)
        
        if submit:
            if username and password:
                success, message = authenticate_user(username, password)
                if success:
                    session_id = create_session(username)
                    st.session_state.session_id = session_id
                    st.session_state.username = username
                    st.session_state.user_role = get_user_role(username)
                    st.success(f"¡Bienvenido, {username}!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(message)
            else:
                st.error("Por favor, complete todos los campos")

def show_info_form():
    """Muestra información para nuevos usuarios"""
    st.markdown("## ℹ️ Información del Sistema")
    st.info("ℹ️ El registro de nuevos usuarios solo puede ser realizado por un administrador.")
    st.markdown("### Usuarios de Prueba:")
    st.write("- **Admin:** `admin` / `admin123`\n- **Gerencia Ventas:** `gerencia_ventas` / `ventas123`\n- **Gerencia Media:** `gerencia_media` / `media123`\n- **Asesor:** `asesor` / `asesor123`")

def show_products_dashboard(user_role):
    """Muestra el dashboard de vehículos según el rol"""
    
    st.markdown("## 🚗 Catálogo de Vehículos")
    
    products_df = load_products()
    
    if products_df.empty:
        st.warning("Aún no hay vehículos registrados.")
        return
        
    st.markdown("---")
    
    # Filtro por familia
    families = ['Todas'] + list(products_df['Familia'].unique())
    selected_family = st.selectbox("Filtrar por Familia", families)
    
    # Obtener lista de archivos históricos para el selector
    historical_files = []
    try:
        historical_files = fs.ls(PRODUCTS_HISTORICAL_PATH)
        historical_files = [os.path.basename(f) for f in historical_files if f.endswith('.csv')]
        historical_files.sort(reverse=True)
    except:
        pass 
    
    col1, col2 = st.columns(2)
    with col1:
        if len(historical_files) >= 2:
            second_last_file = historical_files[1]
            if st.button(f"Ver Precios Anteriores ({second_last_file})", key="btn_old_prices"):
                st.session_state.show_historical_file = second_last_file
                st.rerun()
        else:
            st.button("Ver Precios Anteriores", key="btn_old_prices", disabled=True, help="Necesitas al menos dos archivos históricos para ver una versión anterior.")

    with col2:
        if st.session_state.get('show_historical_file'):
            if st.button("✅ Volver a Precios Actuales", key="btn_current_prices"):
                del st.session_state.show_historical_file
                st.rerun()
    
    df_to_display = pd.DataFrame()

    if 'show_historical_file' in st.session_state:
        st.markdown(f"### 📜 Precios Históricos ({st.session_state.show_historical_file})")
        df_to_display = load_historical_products(st.session_state.show_historical_file)
        if df_to_display.empty:
            st.warning("No se pudo cargar el archivo histórico seleccionado.")
            return
    else:
        st.markdown("### Precios Actuales de Vehículos")
        df_to_display = products_df
    
    # Aplicar el filtro de familia a la tabla a mostrar
    if selected_family != 'Todas':
        df_to_display = df_to_display[df_to_display['Familia'] == selected_family]
        if df_to_display.empty:
            st.warning(f"No hay vehículos de la familia '{selected_family}' disponibles en esta versión de precios.")
            return

    # Visualización de datos
    if user_role == "asesor":
        df_display = df_to_display[['Familia', 'Año', 'Precio_Nibol', 'Precio_Lista', 'Descuento', 'Precio_Final']]
        st.dataframe(df_display, use_container_width=True, hide_index=True)
    else:
        st.dataframe(df_to_display, use_container_width=True, hide_index=True)
        
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Vehículos", len(df_to_display))
    with col2:
        if 'Precio_Final' in df_to_display.columns and not df_to_display.empty:
            avg_price = df_to_display['Precio_Final'].mean()
            st.metric("Precio Final Promedio", f"${avg_price:,.2f}")
    with col3:
        if 'Familia' in df_to_display.columns and not df_to_display.empty:
            st.metric("Familias de Vehículos", df_to_display['Familia'].nunique())

def show_admin_panel():
    """Muestra el panel de administración completo para el rol de admin"""
    st.markdown("## ⚙️ Panel de Administración")
    
    tab1, tab2, tab3 = st.tabs(["Actualizar Precios (CSV)", "Gestión de Usuarios", "Estadísticas"])
    
    with tab1:
        st.markdown("### ⬆️ Subir Archivo de Precios (CSV)")
        uploaded_file = st.file_uploader("Sube un nuevo archivo CSV", type="csv", key="admin_upload")
        if uploaded_file is not None:
            try:
                df_new = pd.read_csv(uploaded_file)
                if st.button("Guardar Nuevos Precios", use_container_width=True, key="admin_save_btn"):
                    save_products(df_new)
                    st.success("Archivo de precios actualizado exitosamente.")
                    time.sleep(1)
                    st.rerun()
            except Exception as e:
                st.error(f"Error al leer el archivo CSV: {e}")

    with tab2:
        st.markdown("### ➕ Dar de Alta un Nuevo Usuario")
        with st.form("create_user_form", clear_on_submit=True):
            new_username = st.text_input("Nombre de Usuario")
            new_password = st.text_input("Contraseña", type="password")
            new_role = st.selectbox("Rol", ["asesor", "gerencia_media", "gerencia_ventas", "admin"])
            
            if st.form_submit_button("Crear Usuario", use_container_width=True):
                if new_username and new_password:
                    success, message = create_user(new_username, new_password, new_role)
                    if success:
                        st.success(message)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(message)
                else:
                    st.warning("El nombre de usuario y la contraseña son obligatorios.")

        st.markdown("---")
        st.markdown("### 📋 Usuarios Registrados")
        users = load_json_file(USERS_FILE, {})
        
        if not users:
            st.info("No hay usuarios registrados.")
        else:
            for username, data in users.items():
                with st.container():
                    col1, col2, col3 = st.columns([2, 2, 1])
                    with col1:
                        st.write(f"**Usuario:** {username}")
                        st.write(f"**Rol:** {data['role']}")
                    with col2:
                        created_date = datetime.fromisoformat(data['created_at']).strftime("%d/%m/%Y %H:%M")
                        st.write(f"**Creado:** {created_date}")
                        if data.get('last_login'):
                            last_login = datetime.fromisoformat(data['last_login']).strftime("%d/%m/%Y %H:%M")
                            st.write(f"**Último acceso:** {last_login}")
                        else:
                            st.write("**Último acceso:** Nunca")
                    with col3:
                        if username == st.session_state.username:
                            st.button("En Sesión", disabled=True, key=f"delete_{username}", use_container_width=True)
                        else:
                            if st.button(f"🗑️ Eliminar", key=f"delete_{username}", use_container_width=True):
                                success, message = delete_user(username)
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(message)
                    st.markdown("---")

    with tab3:
        st.markdown("### Estadísticas del Sistema")
        products_df = load_products()
        users = load_json_file(USERS_FILE, {})
        sessions = load_json_file(SESSIONS_FILE, {})
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Usuarios", len(users))
        with col2:
            st.metric("Sesiones Activas", len(sessions))
        with col3:
            st.metric("Total Vehículos", len(products_df))
            
def show_admin_panel_ventas():
    """Muestra el panel de administración para el rol de gerencia_ventas"""
    st.markdown("## ⚙️ Panel de Administración")
    
    tab1, tab2 = st.tabs(["Actualizar Precios (CSV)", "Estadísticas"])
    
    with tab1:
        st.markdown("### ⬆️ Subir Archivo de Precios (CSV)")
        uploaded_file = st.file_uploader("Sube un nuevo archivo CSV", type="csv", key="ventas_upload")
        if uploaded_file is not None:
            try:
                df_new = pd.read_csv(uploaded_file)
                if st.button("Guardar Nuevos Precios", use_container_width=True, key="ventas_save_btn"):
                    save_products(df_new)
                    st.success("Archivo de precios actualizado exitosamente.")
                    time.sleep(1)
                    st.rerun()
            except Exception as e:
                st.error(f"Error al leer el archivo CSV: {e}")

    with tab2:
        st.markdown("### Estadísticas del Sistema")
        products_df = load_products()
        users = load_json_file(USERS_FILE, {})
        sessions = load_json_file(SESSIONS_FILE, {})
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Usuarios", len(users))
        with col2:
            st.metric("Sesiones Activas", len(sessions))
        with col3:
            st.metric("Total Vehículos", len(products_df))

def show_admin_panel_media():
    """Muestra el panel de administración para el rol de gerencia_media"""
    st.markdown("## ⚙️ Panel de Administración")
    
    tab1, tab2 = st.tabs(["Gestión de Usuarios", "Estadísticas"])
    
    with tab1:
        st.markdown("### 📋 Usuarios Registrados")
        users = load_json_file(USERS_FILE, {})
        
        if not users:
            st.info("No hay usuarios registrados.")
        else:
            for username, data in users.items():
                with st.container():
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Usuario:** {username}")
                        st.write(f"**Rol:** {data['role']}")
                    with col2:
                        created_date = datetime.fromisoformat(data['created_at']).strftime("%d/%m/%Y %H:%M")
                        st.write(f"**Creado:** {created_date}")
                        if data.get('last_login'):
                            last_login = datetime.fromisoformat(data['last_login']).strftime("%d/%m/%Y %H:%M")
                            st.write(f"**Último acceso:** {last_login}")
                        else:
                            st.write("**Último acceso:** Nunca")
                    st.markdown("---")

    with tab2:
        st.markdown("### Estadísticas del Sistema")
        products_df = load_products()
        users = load_json_file(USERS_FILE, {})
        sessions = load_json_file(SESSIONS_FILE, {})
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Usuarios", len(users))
        with col2:
            st.metric("Sesiones Activas", len(sessions))
        with col3:
            st.metric("Total Vehículos", len(products_df))

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