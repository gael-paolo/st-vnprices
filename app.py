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
import traceback

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
        st.write("🔍 Iniciando inicialización de GCS...")
        
        # Verificar si existen los secrets
        if not hasattr(st, 'secrets') or not st.secrets:
            st.error("No se encontraron secrets configurados en Streamlit")
            return False
        
        st.write("✅ Secrets encontrados")
        
        # Obtener las credenciales del formato TOML de Streamlit
        if 'gcp_service_account' in st.secrets:
            service_account_info = dict(st.secrets["gcp_service_account"])
            st.write("✅ Sección gcp_service_account encontrada en secrets")
            
            # Debug: mostrar información de las credenciales (sin datos sensibles)
            st.write(f"📋 Project ID: {service_account_info.get('project_id', 'No encontrado')}")
            st.write(f"📧 Client Email: {service_account_info.get('client_email', 'No encontrado')}")
            st.write(f"🔑 Private Key ID: {service_account_info.get('private_key_id', 'No encontrado')}")
            
            # Asegurarse de que la private_key tenga el formato correcto
            if 'private_key' in service_account_info:
                private_key = service_account_info['private_key']
                st.write(f"📏 Longitud de private_key: {len(private_key)} caracteres")
                
                # Verificar formato de la private key
                if 'BEGIN PRIVATE KEY' in private_key and 'END PRIVATE KEY' in private_key:
                    st.write("✅ Formato de private_key parece correcto")
                    service_account_info['private_key'] = private_key.replace('\\n', '\n')
                else:
                    st.error("❌ Formato de private_key incorrecto")
                    return False
            
            # Crear credenciales
            try:
                credentials = service_account.Credentials.from_service_account_info(service_account_info)
                st.write("✅ Credenciales de servicio creadas")
            except Exception as cred_error:
                st.error(f"❌ Error al crear credenciales: {cred_error}")
                return False
            
            # Inicializar filesystem con diferentes métodos
            try:
                # Método 1: Con token explícito
                fs = gcsfs.GCSFileSystem(
                    project=service_account_info['project_id'], 
                    token=credentials
                )
                st.write("✅ Filesystem inicializado con token")
            except Exception as fs_error:
                st.error(f"❌ Error con método 1: {fs_error}")
                
                # Método 2: Solo con project_id (usará las credenciales por defecto)
                try:
                    fs = gcsfs.GCSFileSystem(project=service_account_info['project_id'])
                    st.write("✅ Filesystem inicializado solo con project_id")
                except Exception as fs_error2:
                    st.error(f"❌ Error con método 2: {fs_error2}")
                    return False
            
            # Test simple de conexión
            try:
                st.write("🔍 Probando conexión con GCS...")
                # Listar buckets para verificar conexión
                buckets = fs.ls('')
                st.write(f"✅ Conexión exitosa. Buckets disponibles: {len(buckets)}")
                
                # Verificar acceso al bucket específico
                if GCS_BUCKET in buckets:
                    st.write(f"✅ Bucket '{GCS_BUCKET}' encontrado")
                    return True
                else:
                    st.error(f"❌ Bucket '{GCS_BUCKET}' no encontrado")
                    st.write(f"Buckets disponibles: {buckets}")
                    return False
                    
            except Exception as test_error:
                st.error(f"❌ Error al conectar con GCS: {test_error}")
                st.write("📋 Traceback completo:")
                st.code(traceback.format_exc())
                return False
                
        else:
            st.error("No se encontró la sección 'gcp_service_account' en los secrets")
            st.write("Secrets disponibles:", list(st.secrets.keys()))
            return False
            
    except Exception as e:
        st.error(f"❌ Error general al inicializar GCS: {e}")
        st.write("📋 Traceback completo:")
        st.code(traceback.format_exc())
        return False

# Mostrar página de debug inicial
st.title("🔧 Debug de Conexión GCS")
st.markdown("---")

# Inicializar GCS
if 'gcs_initialized' not in st.session_state:
    if initialize_gcs():
        st.session_state.gcs_initialized = True
        st.success("🎉 GCS inicializado correctamente!")
    else:
        st.error("❌ No se pudo inicializar GCS")
        st.stop()
else:
    st.success("✅ GCS ya estaba inicializado")

st.markdown("---")
st.subheader("📋 Información de Configuración")

# Mostrar información de configuración
st.write(f"**Bucket:** {GCS_BUCKET}")
st.write(f"**Path:** {GCS_PATH}")
st.write(f"**Archivo de usuarios:** {USERS_FILE}")

# Probamos operaciones básicas
st.markdown("---")
st.subheader("🧪 Pruebas de Operaciones")

if st.button("Probar operaciones GCS"):
    try:
        # Verificar si existe el bucket
        exists = fs.exists(GCS_BUCKET)
        st.write(f"✅ Bucket existe: {exists}")
        
        if exists:
            # Listar contenido del bucket
            contenido = fs.ls(GCS_BUCKET)
            st.write(f"📁 Contenido del bucket: {contenido}")
            
            # Verificar si existe el directorio
            dir_exists = fs.exists(f"{GCS_BUCKET}/{GCS_PATH}")
            st.write(f"📁 Directorio existe: {dir_exists}")
            
    except Exception as e:
        st.error(f"❌ Error en operaciones: {e}")
        st.code(traceback.format_exc())

st.markdown("---")
st.subheader("🔧 Solución de Problemas")

st.write("""
**Posibles soluciones para error 401:**

1. **Verificar permisos del Service Account:**
   - Asegúrate de que el service account tenga permisos de **Storage Admin** o **Storage Object Admin**
   - Verifica que el bucket exista y el SA tenga acceso

2. **Formato de las credenciales:**
   - La private key debe tener saltos de línea correctos (`\\n` → `\n`)
   - Verifica que todos los campos estén completos en los secrets

3. **Probar con acceso público temporal:**
   - Da acceso público al bucket temporalmente para testing
""")

# Solo continuar con la app si GCS está funcionando
if st.session_state.gcs_initialized:
    st.success("✅ Puedes continuar con la aplicación principal")
    if st.button("Continuar a la aplicación"):
        st.session_state.show_app = True
        st.rerun()

if st.session_state.get('show_app'):
    # Aquí iría el resto de tu aplicación...
    st.title("🚗 Sistema de Precios de Vehículos")
    st.write("La aplicación está funcionando correctamente")
else:
    st.info("💡 Resuelve los problemas de conexión antes de continuar")