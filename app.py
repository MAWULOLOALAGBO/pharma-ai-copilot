"""
=============================================================================
PHARMA-AI COPILOT - FICHIER PRINCIPAL V2.0
=============================================================================

Auteur: Mawulolo Koffi Parfait ALAGBO
Date: 2025-02-25
Version: 2.0.0 - Détection intelligente des colonnes

NOUVEAUTÉS CETTE VERSION:
-------------------------
+ Module de détection intelligente des colonnes (utils/schema_detector.py)
+ Analyse automatique du schéma de données
+ Suggestions de noms standardisés
+ Affichage des résultats de détection avec indicateurs de confiance

ARCHITECTURE:
-------------
- Interface utilisateur : Streamlit
- Traitement données : Pandas + Module custom schema_detector
- Visualisations : Plotly (à venir dans v3.0)
- Export : OpenPyXL (à venir dans v4.0)

NOTE:
-----
Ce fichier est volontairement commenté en détail pour faciliter la 
compréhension et la maintenance.
=============================================================================
"""


# =============================================================================
# SECTION 1 : IMPORTS DES BIBLIOTHÈQUES
# =============================================================================

# Streamlit : Framework web pour applications data
import streamlit as st

# Pandas : Manipulation et analyse de données tabulaires
import pandas as pd

# Plotly : Visualisations interactives (préparation pour v3.0)
import plotly.express as px
import plotly.graph_objects as go

# OpenPyXL : Export Excel (préparation pour v4.0)
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

# Python standard library
from datetime import datetime, timedelta
import io
import re
import json
import sys
import os

# =============================================================================
# IMPORTS DES MODULES CUSTOM
# =============================================================================

# Ajout du chemin pour importer les modules utils
# Cette ligne permet de trouver le dossier 'utils' quel que soit le contexte d'exécution
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import du détecteur de schéma intelligent
# Ce module analyse automatiquement les colonnes du fichier uploadé
try:
    from utils.schema_detector import ColumnDetector, format_detection_results
    DETECTOR_AVAILABLE = True
except ImportError as e:
    st.error(f"Erreur d'import du détecteur: {e}")
    DETECTOR_AVAILABLE = False


# =============================================================================
# IMPORTS DES COMPOSANTS DE VISUALISATION
# =============================================================================

try:
    from components.visualizations import render_visualizations, suggest_insights
    VIZ_AVAILABLE = True
except ImportError as e:
    st.error(f"Erreur d'import des visualisations: {e}")
    VIZ_AVAILABLE = False


# Import du générateur Excel (NOUVEAUTÉ V4.0)
try:
    from utils.excel_exporter import generate_excel_report
    EXCEL_AVAILABLE = True
except ImportError as e:
    st.error(f"Erreur d'import de l'export Excel: {e}")
    EXCEL_AVAILABLE = False

# =============================================================================
# SECTION 2 : CONFIGURATION GLOBALE DE L'APPLICATION
# =============================================================================

def configure_app():
    """
    Configure les paramètres globaux de l'application Streamlit.
    """
    
    st.set_page_config(
        page_title="Pharma-AI Copilot",
        page_icon="💊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # CSS amélioré pour meilleur contraste et visibilité
    st.markdown("""
        <style>
        /* Style global */
        .main {
            background-color: #f1f5f9;
            color: #1e293b;
        }
        
        /* Titres avec bon contraste */
        h1 {
            color: #1e40af !important;
            font-weight: 700;
        }
        
        h2, h3 {
            color: #1e3a8a !important;
            font-weight: 600;
        }
        
        /* Métriques plus lisibles */
        [data-testid="stMetricValue"] {
            font-size: 2.2rem !important;
            font-weight: 700 !important;
            color: #1e40af !important;
        }
        
        [data-testid="stMetricLabel"] {
            font-size: 1rem !important;
            color: #475569 !important;
            font-weight: 500 !important;
        }
        
        /* Onglets avec meilleur contraste */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            background-color: transparent;
        }
        
        .stTabs [data-baseweb="tab"] {
            background-color: #e2e8f0 !important;
            border-radius: 8px 8px 0 0 !important;
            padding: 12px 24px !important;
            color: #475569 !important;
            font-weight: 600 !important;
            font-size: 1rem !important;
            border: none !important;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            background-color: #cbd5e1 !important;
            color: #1e40af !important;
        }
        
        .stTabs [aria-selected="true"] {
            background-color: #3b82f6 !important;
            color: white !important;
            border-bottom: 3px solid #1e40af !important;
        }
        
        /* Conteneurs d'export */
        .stAlert {
            background-color: #dbeafe !important;
            border: 1px solid #3b82f6 !important;
            border-radius: 10px !important;
        }
        
        .stAlert > div {
            color: #1e40af !important;
            font-weight: 500 !important;
        }
        
        /* Boutons */
        .stButton > button {
            background-color: #3b82f6 !important;
            color: white !important;
            font-weight: 600 !important;
            border-radius: 8px !important;
            padding: 10px 24px !important;
        }
        
        .stButton > button:hover {
            background-color: #1e40af !important;
        }
        
        button[disabled] {
            background-color: #94a3b8 !important;
            opacity: 0.7 !important;
        }
        
        /* DataFrames */
        .stDataFrame {
            background-color: white !important;
            border-radius: 10px !important;
            border: 1px solid #e2e8f0 !important;
        }
        
        /* Sidebar */
        .css-1d391kg, [data-testid="stSidebar"] {
            background-color: #1e293b !important;
        }
        
        /* Texte dans la sidebar */
        [data-testid="stSidebar"] .stMarkdown {
            color: #e2e8f0 !important;
        }
        
        [data-testid="stSidebar"] label {
            color: #cbd5e1 !important;
        }
        
        /* Footer */
        footer {
            color: #03224c !important;
        }
        
        </style>
    """, unsafe_allow_html=True)


# =============================================================================
# SECTION 3 : FONCTIONS UTILITAIRES (HELPERS)
# =============================================================================

def get_current_timestamp():
    """
    Génère un timestamp formaté pour le nommage des fichiers exportés.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def format_number(number, decimal_places=2):
    """
    Formate un nombre avec séparateurs de milliers et décimales.
    """
    return f"{number:,.{decimal_places}f}".replace(",", " ").replace(".", ",")


# =============================================================================
# SECTION 4 : INTERFACE PRINCIPALE
# =============================================================================

def render_header():
    """
    Affiche l'en-tête de l'application avec titre et description.
    """
    
    with st.container():
        col1, col2 = st.columns([1, 6])
        
        with col1:
            st.markdown("<h1 style='text-align: center; font-size: 4rem;'>💊</h1>", 
                       unsafe_allow_html=True)
        
        with col2:
            st.title("Pharma-AI Copilot")
            st.markdown("""
                **Votre assistant intelligent de gestion de données**
                
                📁 Uploadez n'importe quel fichier (Excel, CSV)  
                🤖 L'IA analyse automatiquement la structure  
                📊 Visualisations et insights auto-générés  
                📥 Export Excel Pro multi-onglets
            """)
        
        st.divider()


def render_sidebar():
    """
    Configure et affiche la barre latérale (sidebar) de navigation.
    
    Returns:
        dict: Configuration sélectionnée par l'utilisateur
    """
    
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Section: Upload de fichier
        st.subheader("📁 Import des données")
        
        uploaded_file = st.file_uploader(
            label="Déposez votre fichier",
            type=['csv', 'xlsx', 'xls'],
            help="Formats supportés: CSV, Excel (.xlsx, .xls)",
            accept_multiple_files=False
        )
        
        # Section: Options d'analyse
        st.subheader("🔍 Options d'analyse")
        
        auto_clean = st.checkbox(
            "Nettoyage automatique des données",
            value=True,
            help="Détecte et corrige automatiquement les erreurs courantes"
        )
        
        detection_level = st.selectbox(
            "Niveau de détection",
            options=["Basique", "Avancé", "Expert"],
            index=1,
            help="Basique: noms de colonnes | Avancé: noms + valeurs | Expert: + IA générative (à venir)"
        )
        
        # Section: Informations
        st.divider()
        st.info("""
            **Version 4.0.0** - Visualisations Auto
            
            Développé par Mawulolo K. P. ALAGBO
            Science des Données - IUT de Vannes
        """)
    
    return {
        'uploaded_file': uploaded_file,
        'auto_clean': auto_clean,
        'detection_level': detection_level
    }


def render_upload_zone(config):
    """
    Affiche la zone principale selon l'état (upload ou analyse).
    
    Args:
        config (dict): Configuration retournée par render_sidebar()
    """
    
    if config['uploaded_file'] is None:
        # Aucun fichier uploadé - afficher l'accueil
        render_welcome_screen()
    
    else:
        # Fichier uploadé - afficher l'analyse
        render_analysis_screen(config)


def render_welcome_screen():
    """
    Affiche l'écran d'accueil quand aucun fichier n'est uploadé.
    """
    
    st.markdown("""
        <div style='text-align: center; padding: 3rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 20px; color: white;'>
            <h2>🚀 Commencez l'analyse</h2>
            <p style='font-size: 1.2rem;'>
                Uploadez votre fichier de stock dans la barre latérale gauche<br>
                pour découvrir la puissance de l'analyse automatique.
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
            ### 🔍 Détection Intelligente
            Reconnaissance automatique des colonnes : produits, quantités, dates, prix...
        """)
    
    with col2:
        st.markdown("""
            ### 🧹 Nettoyage Auto
            Correction des erreurs, suppression des doublons, standardisation des formats
        """)
    
    with col3:
        st.markdown("""
            ### 📊 Insights Actionnables
            Alertes, prévisions et recommandations basées sur vos données réelles
        """)


def render_analysis_screen(config):
    """
    Affiche l'écran d'analyse après upload d'un fichier.
    C'est ici que la magie opère !
    
    Args:
        config (dict): Configuration utilisateur
    """
    
    file = config['uploaded_file']
    
    # ============================================================
    # ÉTAPE 1 : INFORMATIONS DU FICHIER
    # ============================================================
    
    st.success(f"✅ Fichier reçu : **{file.name}**")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        file_size_kb = len(file.getvalue()) / 1024
        st.metric("Taille", f"{file_size_kb:.1f} Ko")
    
    with col2:
        file_extension = file.name.split('.')[-1].upper()
        st.metric("Format", file_extension)
    
    with col3:
        st.metric("Upload", datetime.now().strftime("%H:%M"))
    
    with col4:
        st.metric("Détecteur", "Actif ✅" if DETECTOR_AVAILABLE else "Inactif ❌")
    
    st.divider()
    
    # ============================================================
    # ÉTAPE 2 : LECTURE DU FICHIER
    # ============================================================
    
    try:
            # Lecture selon le format
        if file_extension == 'CSV':
            # Détection automatique du séparateur
            try:
                # Essai avec virgule
                df = pd.read_csv(file, encoding='utf-8', sep=None, engine='python')
            except:
                file.seek(0)
                try:
                    # Essai avec point-virgule (format européen)
                    df = pd.read_csv(file, encoding='utf-8', sep=';')
                except:
                    file.seek(0)
                    df = pd.read_csv(file, encoding='latin-1', sep=None, engine='python')
        else:  # Excel
            df = pd.read_excel(file)
        
        # Affichage des dimensions
        st.info(f"📊 **{len(df)} lignes** × **{len(df.columns)} colonnes** détectées")
        
    except Exception as e:
        st.error(f"❌ Erreur de lecture : {str(e)}")
        st.info("💡 Vérifiez que votre fichier n'est pas corrompu et contient des données tabulaires.")
        return
    
    # ============================================================
    # ÉTAPE 3 : DÉTECTION INTELLIGENTE DU SCHÉMA (NOUVEAUTÉ V2.0)
    # ============================================================
    
    if DETECTOR_AVAILABLE:
        
        st.subheader("🧠 Analyse Intelligente du Schéma")
        
        with st.spinner("L'IA analyse la structure de vos données..."):
            # Instanciation du détecteur
            detector = ColumnDetector()
            
            # Détection du schéma
            schema = detector.detect_schema(df)
            
            # Conversion en DataFrame pour affichage
            detection_df = format_detection_results(schema)
        
        # Affichage des résultats dans un tableau interactif
        st.dataframe(
            detection_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Confiance": st.column_config.TextColumn(
                    "Confiance",
                    help="🟢 >80% = Très fiable | 🟡 50-80% = À vérifier | 🔴 <50% = Incertain"
                )
            }
        )
        
        # Détails par colonne (expandable)
        with st.expander("🔍 Voir les détails complets de la détection"):
            for col_name, meta in schema.items():
                # Détermination de la couleur selon la confiance
                if meta['confidence'] >= 0.8:
                    conf_color = "green"
                    conf_emoji = "🟢"
                elif meta['confidence'] >= 0.5:
                    conf_color = "orange"
                    conf_emoji = "🟡"
                else:
                    conf_color = "red"
                    conf_emoji = "🔴"
                
                # Affichage dans une carte
                st.markdown(f"""
                    <div style='padding: 10px; border-left: 4px solid {conf_color}; background-color: #f9fafb; margin-bottom: 10px; border-radius: 5px;'>
                        <strong>{conf_emoji} {col_name}</strong><br>
                        <small>
                        Type détecté: <b>{meta['detected_type']}</b> | 
                        Technique: <b>{meta['technical_type']}</b> | 
                        Confiance: <b style='color: {conf_color};'>{meta['confidence']:.0%}</b><br>
                        Suggéré: <i>{meta['suggested_name']}</i> | 
                        Uniques: {meta['unique_count']} | 
                        Manquants: {meta['null_count']}
                        </small><br>
                        <small style='color: #6b7280;'>Exemples: {', '.join(map(str, meta['sample_values'][:3]))}</small>
                    </div>
                """, unsafe_allow_html=True)
        
        # ============================================================
        # ÉTAPE 4 : APERÇU DES DONNÉES
        # ============================================================
        
        st.divider()
        st.subheader("📋 Aperçu des données (5 premières lignes)")
        st.dataframe(df.head(), use_container_width=True)
        
        # ============================================================
        # ÉTAPE 4 : STATISTIQUES ET VISUALISATIONS (NOUVEAUTÉ V3.0)
        # ============================================================
        
        if VIZ_AVAILABLE:
            
            # Génération et affichage des visualisations automatiques
            render_visualizations(df, schema)
            
            # Insights textuels générés automatiquement
            st.divider()
            st.subheader("💡 Insights Intelligents")
            
            insights = suggest_insights(df, schema)
            
            for insight in insights:
                st.markdown(f"- {insight}")
            
            # Section export (NOUVEAUTÉ V4.0 - Excel fonctionnel)
            st.divider()
            st.subheader("📤 Export des Résultats")
            
            col_exp1, col_exp2 = st.columns(2)
            
            with col_exp1:
                if EXCEL_AVAILABLE:
                    st.markdown("""
                        <div style='background-color: #dbeafe; padding: 20px; border-radius: 10px; border-left: 4px solid #3b82f6;'>
                            <h4 style='color: #1e40af; margin-top: 0;'>📑 Export Excel Pro</h4>
                            <p style='color: #334155;'>Rapport complet avec 4 onglets : Résumé, Données, Alertes, Analyse</p>
                            <p style='color: #64748b; font-size: 0.9em;'>Mise en forme conditionnelle • Formules • Graphiques</p>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    if st.button("📥 Générer Excel", key="btn_excel"):
                        with st.spinner("Génération du rapport Excel en cours..."):
                            try:
                                # Génération du fichier Excel
                                excel_file = generate_excel_report(df, schema)
                                
                                # Téléchargement
                                st.download_button(
                                    label="⬇️ Télécharger le rapport",
                                    data=excel_file,
                                    file_name=f"pharma_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_excel"
                                )
                                
                                st.success("✅ Rapport Excel généré avec succès !")
                                
                            except Exception as e:
                                st.error(f"❌ Erreur lors de la génération : {str(e)}")
                                st.info("💡 Vérifiez que vos données sont correctement formatées.")
                else:
                    # Fallback si module non disponible
                    st.markdown("""
                        <div style='background-color: #fee2e2; padding: 20px; border-radius: 10px; border-left: 4px solid #ef4444;'>
                            <h4 style='color: #991b1b; margin-top: 0;'>📑 Export Excel Pro</h4>
                            <p style='color: #7f1d1d;'>Module d'export non disponible</p>
                        </div>
                    """, unsafe_allow_html=True)
                    st.button("🔄 Générer Excel", disabled=True, help="Module non chargé", key="btn_excel_disabled")
            
            with col_exp2:
                st.markdown("""
                    <div style='background-color: #e2e8f0; padding: 20px; border-radius: 10px; border-left: 4px solid #94a3b8;'>
                        <h4 style='color: #475569; margin-top: 0;'>📄 Export PDF</h4>
                        <p style='color: #64748b;'>Rapport PDF avec graphiques et insights</p>
                        <p style='color: #94a3b8; font-size: 0.9em;'><i>Disponible dans une future version</i></p>
                    </div>
                """, unsafe_allow_html=True)
                st.button("🔄 Générer PDF", disabled=True, help="Bientôt disponible", key="btn_pdf")
        
        else:
            # Fallback si visualisations non disponibles
            st.divider()
            st.subheader("📋 Aperçu des données")
            st.dataframe(df.head(10), use_container_width=True)
            
            st.info("Module de visualisation en cours de chargement...")
    
    else:
        # Détecteur non disponible - affichage basique
        st.warning("⚠️ Module de détection non disponible. Affichage basique uniquement.")
        st.subheader("📋 Aperçu brut des données")
        st.dataframe(df.head(), use_container_width=True)


# =============================================================================
# SECTION 5 : POINT D'ENTRÉE PRINCIPAL (MAIN)
# =============================================================================

def main():
    """
    Fonction principale qui orchestre toute l'application.
    """
    
    # Étape 1: Configuration
    configure_app()
    
    # Étape 2: Header
    render_header()
    
    # Étape 3: Sidebar et configuration utilisateur
    user_config = render_sidebar()
    
    # Étape 4: Zone principale dynamique
    render_upload_zone(user_config)
    
    # Footer
    st.divider()
    st.markdown("""
        <div style='text-align: center; color: #64748b; padding: 20px; font-size: 0.9rem;'>
            © 2025 Pharma-AI Copilot v4.0.0 | Développé avec ❤️ en France
        </div>
    """, unsafe_allow_html=True)


# =============================================================================
# EXÉCUTION DU SCRIPT
# =============================================================================

if __name__ == "__main__":
    main()
