"""
=============================================================================
PHARMA-AI COPILOT - FICHIER PRINCIPAL
=============================================================================

Auteur: Mawulolo Koffi Parfait ALAGBO
Date: 2025-02-25
Version: 1.0.0 - Structure de base

DESCRIPTION:
------------
Cette application Streamlit est un outil d'analyse intelligente pour 
pharmaciens. Elle permet d'uploader des fichiers de stock, de les analyser
automatiquement et de générer des insights actionnables.

ARCHITECTURE:
-------------
- Interface utilisateur : Streamlit (web app)
- Traitement données : Pandas (manipulation DataFrame)
- Visualisations : Plotly (graphiques interactifs)
- Export : OpenPyXL (fichiers Excel)

STRUCTURE DU CODE:
------------------
1. IMPORTS : Toutes les bibliothèques nécessaires
2. CONFIGURATION : Paramètres globaux de l'app
3. FONCTIONS UTILITAIRES : Helper functions réutilisables
4. INTERFACE PRINCIPALE : Layout et composants UI
5. LOGIQUE MÉTIER : Traitement des données et analyses

NOTE:
-----
Ce fichier est volontairement commenté en détail pour faciliter la 
compréhension et la maintenance par le développeur.
=============================================================================
"""


# =============================================================================
# SECTION 1 : IMPORTS DES BIBLIOTHÈQUES
# =============================================================================

# Streamlit : Framework web pour applications data (gratuit, open-source)
# Documentation : https://docs.streamlit.io/
import streamlit as st

# Pandas : Manipulation et analyse de données tabulaires
# C'est l'outil standard en Python pour traiter des fichiers Excel/CSV
import pandas as pd

# Plotly Express : Création de graphiques interactifs rapidement
# Plotly Graph Objects : Graphiques avancés et personnalisables
import plotly.express as px
import plotly.graph_objects as go

# OpenPyXL : Lecture et écriture de fichiers Excel (.xlsx)
# Permet de créer des fichiers Excel avec mise en forme, formules, etc.
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

# Python standard library : Fonctions natives de Python
from datetime import datetime, timedelta  # Gestion des dates
import io  # Manipulation de flux de données (fichiers en mémoire)
import re  # Expressions régulières (recherche de patterns dans texte)
import json  # Manipulation de données JSON


# =============================================================================
# SECTION 2 : CONFIGURATION GLOBALE DE L'APPLICATION
# =============================================================================

def configure_app():
    """
    Configure les paramètres globaux de l'application Streamlit.
    Cette fonction doit être appelée en premier dans le script.
    
    Returns:
        None
    """
    
    # Configuration de la page (titre, icône, layout)
    # 'page_title' : Titre affiché dans l'onglet du navigateur
    # 'page_icon' : Emoji ou image affiché dans l'onglet
    # 'layout' : 'wide' = utilisation maximale de l'espace écran
    # 'initial_sidebar_state' : Sidebar ouverte par défaut
    st.set_page_config(
        page_title="Pharma-AI Copilot",
        page_icon="💊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Injection de CSS personnalisé pour le style "Glassmorphism"
    # Ce style donne un effet de vitre translucide moderne
    st.markdown("""
        <style>
        /* Style global de l'application */
        .main {
            background-color: #f8fafc;
        }
        
        /* Effet glassmorphism pour les cartes */
        .glass-card {
            background: rgba(255, 255, 255, 0.7);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            border: 1px solid rgba(255, 255, 255, 0.3);
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        /* Style des titres */
        h1 {
            color: #1e40af;
            font-weight: 700;
        }
        
        h2, h3 {
            color: #1e3a8a;
            font-weight: 600;
        }
        </style>
    """, unsafe_allow_html=True)


# =============================================================================
# SECTION 3 : FONCTIONS UTILITAIRES (HELPERS)
# =============================================================================

def get_current_timestamp():
    """
    Génère un timestamp formaté pour le nommage des fichiers exportés.
    
    Returns:
        str: Timestamp au format YYYYMMDD_HHMMSS
        Exemple: "20250225_143052"
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def format_number(number, decimal_places=2):
    """
    Formate un nombre avec séparateurs de milliers et décimales.
    
    Args:
        number (float/int): Le nombre à formater
        decimal_places (int): Nombre de décimales souhaitées
    
    Returns:
        str: Nombre formaté (ex: "1 234,56")
    """
    return f"{number:,.{decimal_places}f}".replace(",", " ").replace(".", ",")


# =============================================================================
# SECTION 4 : INTERFACE PRINCIPALE
# =============================================================================

def render_header():
    """
    Affiche l'en-tête de l'application avec titre et description.
    Cette fonction crée la première impression visuelle.
    """
    
    # Container principal pour le header
    with st.container():
        # Colonnes pour alignement logo + titre
        col1, col2 = st.columns([1, 6])
        
        with col1:
            # Emoji grande taille comme logo temporaire
            st.markdown("<h1 style='text-align: center; font-size: 4rem;'>💊</h1>", 
                       unsafe_allow_html=True)
        
        with col2:
            # Titre principal avec style
            st.title("Pharma-AI Copilot")
            # Sous-titre explicatif
            st.markdown("""
                **Votre assistant intelligent de gestion de stock pharmaceutique**
                
                📁 Uploadez n'importe quel fichier (Excel, CSV)  
                🤖 Laissez l'IA analyser et comprendre vos données  
                📊 Obtenez des insights actionnables et des exports Excel pro
            """)
        
        # Ligne de séparation visuelle
        st.divider()


def render_sidebar():
    """
    Configure et affiche la barre latérale (sidebar) de navigation.
    C'est ici que seront placés les contrôles principaux.
    
    Returns:
        dict: Configuration sélectionnée par l'utilisateur
    """
    
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Section: Upload de fichier
        st.subheader("📁 Import des données")
        
        # Widget d'upload de fichier
        # 'accept_multiple_files=False' : Un seul fichier à la fois pour l'instant
        uploaded_file = st.file_uploader(
            label="Déposez votre fichier de stock",
            type=['csv', 'xlsx', 'xls'],  # Extensions acceptées
            help="Formats supportés: CSV, Excel (.xlsx, .xls)",
            accept_multiple_files=False
        )
        
        # Section: Options d'analyse
        st.subheader("🔍 Options d'analyse")
        
        # Checkbox pour activer/désactiver le nettoyage auto
        auto_clean = st.checkbox(
            "Nettoyage automatique des données",
            value=True,
            help="Détecte et corrige automatiquement les erreurs courantes"
        )
        
        # Sélection du niveau de détection
        detection_level = st.selectbox(
            "Niveau de détection intelligente",
            options=["Basique", "Avancé", "Expert"],
            index=1,
            help="Basique: noms de colonnes standards | Avancé: inférences contextuelles | Expert: IA générative"
        )
        
        # Section: Informations
        st.divider()
        st.info("""
            **Version 1.0.0** - MVP
            
            Développé par Mawulolo K. P. ALAGBO
            Science des Données - IUT de Vannes
        """)
    
    # Retourne les paramètres sélectionnés sous forme de dictionnaire
    return {
        'uploaded_file': uploaded_file,
        'auto_clean': auto_clean,
        'detection_level': detection_level
    }


def render_upload_zone(config):
    """
    Affiche la zone principale d'upload et les instructions.
    
    Args:
        config (dict): Configuration retournée par render_sidebar()
    """
    
    # Si aucun fichier n'est uploadé, afficher la zone d'accueil
    if config['uploaded_file'] is None:
        
        # Grande zone centrale avec instructions
        st.markdown("""
            <div style='text-align: center; padding: 3rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 20px; color: white;'>
                <h2>🚀 Commencez l'analyse</h2>
                <p style='font-size: 1.2rem;'>
                    Uploadez votre fichier de stock dans la barre latérale gauche<br>
                    pour découvrir la puissance de l'analyse automatique.
                </p>
            </div>
        """, unsafe_allow_html=True)
        
        # Exemple de ce que l'outil peut faire (3 colonnes)
        st.divider()
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
                ### 📊 Visualisations Auto
                Graphiques interactifs générés automatiquement selon vos données
            """)
        
        with col2:
            st.markdown("""
                ### 🔍 Détection Intelligente
                Reconnaissance automatique des colonnes (médicaments, quantités, dates)
            """)
        
        with col3:
            st.markdown("""
                ### 📑 Export Excel Pro
                Fichiers Excel formatés avec onglets, formules et mise en forme
            """)
    
    else:
        # Fichier uploadé : afficher les informations de base
        file = config['uploaded_file']
        
        st.success(f"✅ Fichier reçu : **{file.name}**")
        
        # Informations techniques sur le fichier
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Taille du fichier en Ko
            file_size_kb = len(file.getvalue()) / 1024
            st.metric("Taille du fichier", f"{file_size_kb:.1f} Ko")
        
        with col2:
            # Extension détectée
            file_extension = file.name.split('.')[-1].upper()
            st.metric("Format détecté", file_extension)
        
        with col3:
            # Timestamp de l'upload
            st.metric("Heure d'upload", datetime.now().strftime("%H:%M:%S"))
        
        # Aperçu brut (sera remplacé par l'analyse intelligente plus tard)
        st.subheader("🔍 Aperçu brut des données (5 premières lignes)")
        
        try:
            # Lecture du fichier selon son extension
            if file_extension == 'CSV':
                df_preview = pd.read_csv(file, nrows=5)
            else:  # Excel
                df_preview = pd.read_excel(file, nrows=5)
            
            # Affichage du DataFrame
            st.dataframe(df_preview, use_container_width=True)
            
            # Information sur les colonnes détectées
            st.info(f"📋 **{len(df_preview.columns)} colonnes détectées** : {', '.join(df_preview.columns)}")
            
        except Exception as e:
            # Gestion d'erreur si le fichier ne peut pas être lu
            st.error(f"❌ Erreur de lecture du fichier : {str(e)}")
            st.info("💡 Conseil : Vérifiez que votre fichier n'est pas corrompu et qu'il contient des données tabulaires.")


# =============================================================================
# SECTION 5 : POINT D'ENTRÉE PRINCIPAL (MAIN)
# =============================================================================

def main():
    """
    Fonction principale qui orchestre toute l'application.
    C'est le point d'entrée exécuté au démarrage.
    
    Ordre d'exécution :
    1. Configuration de l'app
    2. Affichage du header
    3. Affichage de la sidebar et récupération config
    4. Affichage de la zone principale selon l'état
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
    st.caption("© 2025 Pharma-AI Copilot - Propulsé par Streamlit | Développé avec ❤️ en France")


# =============================================================================
# EXÉCUTION DU SCRIPT
# =============================================================================

# Cette condition vérifie si le script est exécuté directement (pas importé)
# C'est une bonne pratique Python pour éviter l'exécution lors des imports
if __name__ == "__main__":
    main()
