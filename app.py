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
from google.auth import default
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
            
            # Método 1: Usar Application Default Credentials (ADC)
            try:
                st.write("🔄 Probando con Application Default Credentials...")
                # Forzar el uso de las credenciales proporcionadas
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                st.write("✅ Credenciales creadas con scopes explícitos")
                
                # Inicializar filesystem
                fs = gcsfs.GCSFileSystem(
                    project=service_account_info['project_id'], 
                    token=credentials
                )
                st.write("✅ Filesystem inicializado")
                
            except Exception as method1_error:
                st.error(f"❌ Error con método 1: {method1_error}")
                
                # Método 2: Usar credenciales por defecto del entorno
                try:
                    st.write("🔄 Probando con credenciales por defecto...")
                    credentials, project = default()
                    fs = gcsfs.GCSFileSystem(project=service_account_info['project_id'])
                    st.write("✅ Filesystem inicializado con credenciales por defecto")
                    
                except Exception as method2_error:
                    st.error(f"❌ Error con método 2: {method2_error}")
                    
                    # Método 3: Usar solo project_id (para cuando las credenciales están en el entorno)
                    try:
                        st.write("🔄 Probando solo con project_id...")
                        fs = gcsfs.GCSFileSystem(project=service_account_info['project_id'])
                        st.write("✅ Filesystem inicializado solo con project_id")
                        
                    except Exception as method3_error:
                        st.error(f"❌ Error con método 3: {method3_error}")
                        return False
            
            # Test simple de conexión
            try:
                st.write("🔍 Probando conexión con GCS...")
                # Listar buckets para verificar conexión
                buckets = fs.ls('')
                st.write(f"✅ Conexión exitosa. Buckets disponibles: {len(buckets)}")
                
                # Verificar acceso al bucket específico
                bucket_names = [b.split('/')[-1] for b in buckets if b.endswith('/')]
                if GCS_BUCKET in bucket_names:
                    st.write(f"✅ Bucket '{GCS_BUCKET}' encontrado")
                    return True
                else:
                    st.error(f"❌ Bucket '{GCS_BUCKET}' no encontrado")
                    st.write(f"Buckets disponibles: {bucket_names}")
                    return False
                    
            except Exception as test_error:
                st.error(f"❌ Error al conectar con GCS: {test_error}")
                return False
                
        else:
            st.error("No se encontró la sección 'gcp_service_account' en los secrets")
            return False
            
    except Exception as e:
        st.error(f"❌ Error general al inicializar GCS: {e}")
        st.code(traceback.format_exc())
        return False

# Función alternativa para inicializar GCS (más simple)
def initialize_gcs_simple():
    """Inicialización simple de GCS"""
    global fs
    try:
        # Usar las credenciales por defecto del entorno
        fs = gcsfs.GCSFileSystem(project="nissan-435902")
        return True
    except Exception as e:
        st.error(f"Error simple: {e}")
        return False

# Mostrar página de debug inicial
st.title("🔧 Debug de Conexión GCS")
st.markdown("---")

# Opción para probar diferentes métodos
method = st.radio("Selecciona método de conexión:", 
                 ["Automático", "Solo project_id", "Con credenciales explícitas"])

if st.button("🔌 Probar Conexión"):
    if method == "Automático":
        success = initialize_gcs()
    elif method == "Solo project_id":
        success = initialize_gcs_simple()
    else:
        success = initialize_gcs()
    
    if success:
        st.session_state.gcs_initialized = True
        st.success("🎉 GCS inicializado correctamente!")
    else:
        st.error("❌ No se pudo inicializar GCS")

st.markdown("---")

if st.session_state.get('gcs_initialized'):
    st.success("✅ GCS inicializado - Probando operaciones...")
    
    # Probar operaciones básicas
    try:
        # Verificar si existe el bucket
        exists = fs.exists(GCS_BUCKET)
        st.write(f"📦 Bucket existe: {exists}")
        
        if exists:
            # Listar contenido del bucket
            try:
                contenido = fs.ls(GCS_BUCKET)
                st.write(f"📁 Contenido del bucket: {contenido}")
            except Exception as e:
                st.error(f"❌ Error al listar bucket: {e}")
            
            # Verificar si existe el directorio
            dir_path = f"{GCS_BUCKET}/{GCS_PATH}"
            dir_exists = fs.exists(dir_path)
            st.write(f"📂 Directorio existe: {dir_exists}")
            
            if not dir_exists:
                st.info("ℹ️ El directorio no existe, se creará automáticamente")
            
            # Probar escritura
            try:
                test_file = f"{GCS_BUCKET}/test_connection.txt"
                with fs.open(test_file, 'w') as f:
                    f.write(f"Test de conexión exitoso - {datetime.now()}")
                st.success("✅ Escritura exitosa")
                
                # Limpiar archivo de test
                if fs.exists(test_file):
                    fs.rm(test_file)
                    
            except Exception as e:
                st.error(f"❌ Error de escritura: {e}")
    
    except Exception as e:
        st.error(f"❌ Error en operaciones: {e}")

st.markdown("---")
st.subheader("🔧 Solución de Problemas - Error 401")

st.write("""
**Para resolver el error 'invalid_scope':**

1. **Verificar el Service Account en Google Cloud Console:**
   - Ve a IAM & Admin > Service Accounts
   - Asegúrate de que el SA tenga el rol **Storage Admin**
   - Verifica que esté habilitado

2. **Verificar formato de credenciales en Streamlit Secrets:**
   - La private key debe tener saltos de línea correctos
   - Todos los campos deben estar completos

3. **Probar acceso directo:**
   - Temporalmente da acceso público al bucket para testing
   - O crea un nuevo Service Account con permisos mínimos

4. **Scopes alternativos:**
   - El error sugiere un problema con los scopes OAuth
   - Probamos con el scope completo de cloud-platform
""")

# Configuración alternativa para desarrollo
st.markdown("---")
st.subheader("🛠️ Configuración Alternativa")

if st.button("🔄 Reinicializar GCS"):
    if 'gcs_initialized' in st.session_state:
        del st.session_state.gcs_initialized
    st.rerun()

# Solo continuar con la app si GCS está funcionando
if st.session_state.get('gcs_initialized'):
    st.success("✅ Puedes continuar con la aplicación principal")
    if st.button("🚗 Continuar a la aplicación"):
        st.session_state.show_app = True
        st.rerun()

if st.session_state.get('show_app'):
    # Aquí iría el resto de tu aplicación...
    st.title("🚗 Sistema de Precios de Vehículos")
    st.write("La aplicación está funcionando correctamente")
else:
    st.info("💡 Resuelve los problemas de conexión antes de continuar")