"""
=============================================================================
MODULE : schema_detector.py
=============================================================================

DESCRIPTION :
-------------
Ce module contient la logique de détection intelligente des colonnes.
Il analyse chaque colonne d'un DataFrame pour déterminer :
1. Le type technique (string, int, float, date, bool)
2. La catégorie sémantique (produit, quantité, prix, date, etc.)
3. La confiance de la détection (score 0-100)

MÉTHODOLOGIE :
--------------
- Analyse des noms de colonnes (pattern matching, keywords)
- Analyse des valeurs (regex, statistiques, distribution)
- Scoring pondéré combinant les deux approches

AUTEUR : Mawulolo Koffi Parfait ALAGBO
DATE : 2025-02-25
=============================================================================
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Dict, List, Tuple, Any


class ColumnDetector:
    """
    Classe principale pour la détection intelligente des colonnes.
    
    Cette classe encapsule toute la logique d'analyse et fournit
    une API simple : detect_schema(df) -> dict des résultats.
    """
    
    def __init__(self):
        """
        Initialise les patterns et dictionnaires de détection.
        """
        
        # =================================================================
        # DICTIONNAIRES DE MOTS-CLÉS PAR CATÉGORIE
        # =================================================================
        
        # Mots-clés pour les noms de produits/médicaments
        self.product_keywords = [
            'produit', 'médicament', 'medicament', 'drug', 'product',
            'nom', 'name', 'désignation', 'designation', 'libellé', 'libelle',
            'détail', 'detail', 'description', 'article', 'item', 'dénomination',
            'intitulé', 'label', 'title', 'dénomination', 'appellation'
        ]
        
        # Mots-clés pour les codes (CIP, EAN, etc.)
        self.code_keywords = [
            'code', 'cip', 'ean', 'gtin', 'ref', 'réf', 'reference', 'référence',
            'sku', 'id', 'identifiant', 'numéro', 'numero', 'n°', 'num',
            'barcode', 'barre', 'cle', 'clé', 'key', 'code commande', 'référence fabricant'
        ]
        
        # Mots-clés pour les quantités/stocks
        self.quantity_keywords = [
            'quantité', 'quantite', 'qty', 'quantity', 'stock', 'qte', 'qté',
            'nombre', 'number', 'count', 'volume', 'unit', 'unité', 'nb',
            'total', 'disponible', 'dispo', 'en stock', 'inventory', 'level',
            'amount', 'somme', 'cumulative'
        ]
        
        # Mots-clés pour les prix
        self.price_keywords = [
            'prix', 'price', 'cost', 'coût', 'cout', 'tarif', 'montant',
            'amount', 'valeur', 'value', 'total', 'ht', 'ttc', 'eur', '€',
            'euro', 'dollar', '$', 'usd', 'achat', 'vente', 'revient',
            'unitaire', 'unit price', 'prix unitaire'
        ]
        
        # Mots-clés pour les dates
        self.date_keywords = [
            'date', 'time', 'jour', 'day', 'mois', 'month', 'année', 'year',
            'péremption', 'peremption', 'expiration', 'expiry', 'validité',
            'validity', 'dluo', 'dlc', 'fab', 'fabrication', 'production',
            'réception', 'reception', 'commande', 'livraison', 'creation',
            'mise à jour', 'update', 'timestamp', 'created', 'updated'
        ]
        
        # Mots-clés pour les catégories
        self.category_keywords = [
            'catégorie', 'categorie', 'category', 'type', 'famille', 'family',
            'classe', 'class', 'groupe', 'group', 'secteur', 'sector',
            'domaine', 'field', 'nature', 'genre', 'kind', 'classification',
            'therapeutique', 'thérapeutique', 'atc', 'classe thérapeutique'
        ]
        
        # Mots-clés pour les marques/fournisseurs
        self.brand_keywords = [
            'marque', 'brand', 'fournisseur', 'supplier', 'vendor', 'fabricant',
            'manufacturer', 'maker', 'producteur', 'producer', 'labo',
            'laboratoire', 'pharma', 'pharmaceutique', 'société', 'company',
            'entreprise', 'enseigne', 'enseigne', 'siemens', 'kimo', 'wago', 'jumo'
        ]
        
        # =================================================================
        # PATTERNS REGEX POUR DÉTECTION DE FORMATS
        # =================================================================
        
        # Pattern pour codes CIP (Code Identifiant du Préparateur) - 13 chiffres
        self.cip_pattern = re.compile(r'^\d{13}$')
        
        # Pattern pour codes EAN-13 (souvent commencent par 340 pour la pharma FR)
        self.ean_pattern = re.compile(r'^3\d{12}$')
        
        # Pattern pour dates (formats variés)
        self.date_patterns = [
            re.compile(r'^\d{2}/\d{2}/\d{4}$'),      # DD/MM/YYYY
            re.compile(r'^\d{4}-\d{2}-\d{2}$'),      # YYYY-MM-DD
            re.compile(r'^\d{2}-\d{2}-\d{4}$'),      # DD-MM-YYYY
            re.compile(r'^\d{2}/\d{2}/\d{2}$'),      # DD/MM/YY
            re.compile(r'^\d{1,2}/\d{1,2}/\d{2,4}$'), # Flexible
        ]
        
        # Pattern pour prix (nombre avec 2 décimales ou symbole €)
        self.price_pattern = re.compile(r'^\d+[.,]?\d{0,2}\s*[€$]?$')
        
        # Pattern pour quantités (nombres entiers positifs)
        self.quantity_pattern = re.compile(r'^\d+$')
        
        # Pattern pour URLs
        self.url_pattern = re.compile(r'^https?://')
    
    # =========================================================================
    # MÉTHODES PUBLIQUES PRINCIPALES
    # =========================================================================
    
    def detect_schema(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Méthode principale : analyse tout le DataFrame et retourne
        un dictionnaire avec les métadonnées de chaque colonne.
        
        Args:
            df (pd.DataFrame): Le DataFrame à analyser
        
        Returns:
            dict: {
                'nom_colonne': {
                    'detected_type': str,      # 'product', 'code', 'quantity', etc.
                    'technical_type': str,     # 'string', 'int', 'float', 'date'
                    'confidence': float,       # 0.0 à 1.0
                    'suggested_name': str,     # Nom standardisé suggéré
                    'sample_values': list,     # 3 valeurs d'exemple
                    'null_count': int,         # Nombre de valeurs manquantes
                    'unique_count': int        # Nombre de valeurs uniques
                },
                ...
            }
        """
        schema = {}
        
        # Analyse chaque colonne
        for column_name in df.columns:
            column_data = df[column_name]
            
            # Détection pour cette colonne
            detection_result = self._analyze_column(column_name, column_data)
            schema[column_name] = detection_result
        
        return schema
    
    def get_summary(self, schema: Dict) -> str:
        """
        Génère un résumé textuel lisible du schéma détecté.
        
        Args:
            schema (dict): Résultat de detect_schema()
        
        Returns:
            str: Résumé formaté pour affichage
        """
        lines = ["📊 DÉTECTION DU SCHÉMA", "=" * 50, ""]
        
        for col_name, meta in schema.items():
            conf_emoji = "🟢" if meta['confidence'] > 0.8 else "🟡" if meta['confidence'] > 0.5 else "🔴"
            lines.append(f"{conf_emoji} **{col_name}**")
            lines.append(f"   → Type détecté: {meta['detected_type']} ({meta['technical_type']})")
            lines.append(f"   → Confiance: {meta['confidence']:.0%}")
            lines.append(f"   → Exemples: {', '.join(map(str, meta['sample_values']))}")
            lines.append("")
        
        return "\n".join(lines)
    
    # =========================================================================
    # MÉTHODES PRIVÉES D'ANALYSE
    # =========================================================================
    
    def _analyze_column(self, col_name: str, col_data: pd.Series) -> Dict[str, Any]:
        """
        Analyse une colonne individuelle (nom + valeurs).
        """
        # Nettoyage du nom de colonne pour analyse
        clean_name = col_name.lower().strip()
        
        # 1. Analyse du nom de colonne (score basé sur mots-clés)
        name_score = self._score_column_name(clean_name)
        
        # 2. Analyse des valeurs (score basé sur contenu)
        value_score, technical_type = self._score_column_values(col_data)
        
        # 3. Combinaison des scores avec pondération
        # Le nom compte pour 40%, les valeurs pour 60%
        final_scores = {}
        for category in name_score.keys():
            name_conf = name_score.get(category, 0)
            val_conf = value_score.get(category, 0)
            final_scores[category] = (name_conf * 0.4) + (val_conf * 0.6)
        
        # 4. Sélection de la catégorie gagnante
        if final_scores:
            detected_type = max(final_scores, key=final_scores.get)
            confidence = final_scores[detected_type]
        else:
            detected_type = 'unknown'
            confidence = 0.0
        
        # 5. Détermination du nom standardisé suggéré
        suggested_name = self._get_standardized_name(detected_type, clean_name)
        
        # 6. Statistiques sur les données
        sample_values = col_data.dropna().head(3).tolist()
        null_count = col_data.isna().sum()
        unique_count = col_data.nunique()
        
        return {
            'detected_type': detected_type,
            'technical_type': technical_type,
            'confidence': round(confidence, 2),
            'suggested_name': suggested_name,
            'sample_values': sample_values,
            'null_count': int(null_count),
            'unique_count': int(unique_count),
            'all_scores': final_scores  # Pour debug
        }
    
    def _score_column_name(self, name: str) -> Dict[str, float]:
        """
        Attribue un score à chaque catégorie basé sur le nom de colonne.
        """
        scores = {}
        
        # Nettoyage du nom
        clean_name = name.lower().strip().replace('_', '').replace(' ', '')
        
        # DÉTECTION PRIORITAIRE : IDs (à ajouter ici, au début)
        
        if any(keyword in clean_name for keyword in ['idpharmacie', 'idpharmacy', 'idproduit', 'idproduct', 'idclient', 'idpatient']):
            scores['code'] = 0.95  # ID = code, pas quantity
            scores['quantity'] = 0.0  # Éviter confusion
            return scores  # On retourne immédiatement, c'est un ID
        
        # RESTE DE LA FONCTION (déjà existant)
        
        # Fonction helper pour calculer le score d'une catégorie
        def calc_score(keywords, weight=1.0):
            score = 0.0
            for keyword in keywords:
                if keyword in clean_name:
                    if name.lower().strip() == keyword:
                        score += 1.0 * weight
                    elif clean_name.startswith(keyword):
                        score += 0.8 * weight
                    else:
                        score += 0.5 * weight
            return min(score, 1.0)
        
        scores['product'] = calc_score(self.product_keywords)
        scores['code'] = calc_score(self.code_keywords)
        scores['quantity'] = calc_score(self.quantity_keywords)
        scores['price'] = calc_score(self.price_keywords)
        scores['date'] = calc_score(self.date_keywords)
        scores['category'] = calc_score(self.category_keywords)
        scores['brand'] = calc_score(self.brand_keywords)

            # Priorité forte pour "designation" (nom de produit en pharma)
        if 'designation' in clean_name:
            scores['product'] = 1.0
            scores['brand'] = 0.0  # Éviter confusion avec brand
        
        return scores
    
    def _score_column_values(self, col_data: pd.Series) -> Tuple[Dict[str, float], str]:
        """
        Analyse les valeurs avec PRIORITÉ aux codes sur les nombres.
        """
        scores = {}
        
        values = col_data.dropna().astype(str)
        total_values = len(values)
        
        if total_values == 0:
            return {}, 'empty'
        
        # Type technique pandas
        dtype = col_data.dtype
        
        # =================================================================
        # DÉTECTION PRIORITAIRE : CODES ET IDENTIFIANTS
        # =================================================================
        
        # Pattern pour codes CIP (13 chiffres, souvent avec virgules ou espaces)
        cip_pattern = re.compile(r'^\d{1,3}(?:[ ,.]?\d{3})*$')  # Gère 7 084 133 ou 7,084,133
        cip_matches = values.str.match(cip_pattern).sum()
        
        # Vérification : est-ce que ça ressemble à des codes CIP (longueur 7-13 chiffres)
        avg_length = values.str.replace('[ ,.]', '', regex=True).str.len().mean()
        looks_like_cip = 7 <= avg_length <= 13 and values.str.match(r'^\d+$').sum() / total_values > 0.8
        
        if looks_like_cip:
            scores['code'] = 0.95  # Très forte confiance pour CIP
            return scores, 'string' if dtype == 'object' else 'int'
        
        # Pattern pour IDs (entiers courts, séquentiels)
        id_pattern = re.compile(r'^\d{1,6}$')
        id_matches = values.str.match(id_pattern).sum()
        unique_ratio = col_data.nunique() / total_values
        
        # ID = entiers courts, peu de répétition, nom de colonne contient "id"
        if id_matches / total_values > 0.9 and unique_ratio > 0.8:
            scores['code'] = 0.90
            return scores, 'int'
        
        # =================================================================
        # DÉTECTION STANDARD (si pas de code identifié)
        # =================================================================
        
        if pd.api.types.is_datetime64_any_dtype(col_data):
            technical_type = 'date'
            scores['date'] = 1.0
        elif pd.api.types.is_integer_dtype(col_data):
            technical_type = 'int'
        elif pd.api.types.is_float_dtype(col_data):
            technical_type = 'float'
        else:
            technical_type = 'string'
        
        # Prix : doit contenir des décimales OU symboles €
        if technical_type == 'float' or (technical_type == 'string' and values.str.contains('€|\$|EUR|euro', case=False, regex=True).sum() / total_values > 0.3):
            scores['price'] = 0.90
        elif technical_type in ['int', 'float']:
            # Quantité : entiers positifs raisonnables (pas des millions)
            numeric_values = pd.to_numeric(col_data.dropna(), errors='coerce')
            if (numeric_values >= 0).all() and (numeric_values <= 100000).mean() > 0.95:
                scores['quantity'] = 0.85
            else:
                scores['quantity'] = 0.3
        
        # Catégorie / Produit / Marque (analyse textuelle)
        if technical_type == 'string':
            # ... (reste de la logique existante)
            pass
        
        return scores, technical_type
    
    def _get_standardized_name(self, detected_type: str, original_name: str) -> str:
        """
        Retourne un nom standardisé OU garde l'original s'il est déjà clair.
        """
        # Si le nom original est déjà explicite, on le garde
        explicit_names = {
            'stock_actuel': 'Stock Actuel',
            'stock_min': 'Stock Minimum',
            'stock_max': 'Stock Maximum',
            'stock_securite': 'Stock Sécurité',
            'quantite': 'Quantité',
            'qte': 'Quantité',
            'prix_achat': "Prix d'Achat",
            'prix_vente': 'Prix de Vente',
            'prix_unitaire': 'Prix Unitaire',
            'code_cip': 'Code CIP',
            'code_ean': 'Code EAN',
            'id_produit': 'ID Produit',
            'id_pharmacie': 'ID Pharmacie',
        }
        
        original_lower = original_name.lower().replace('_', '').replace(' ', '')
        
        for key, value in explicit_names.items():
            if key.replace('_', '') in original_lower or original_lower in key.replace('_', ''):
                return value
        
        # Sinon mapping générique
        mapping = {
            'product': 'Produit',
            'code': 'Code',
            'quantity': 'Quantité',
            'price': 'Prix',
            'date': 'Date',
            'category': 'Catégorie',
            'brand': 'Marque/Fabricant',
            'url': 'URL',
            'unknown': original_name
        }
        return mapping.get(detected_type, original_name)


# =============================================================================
# FONCTIONS UTILITAIRES SIMPLIFIÉES
# =============================================================================

def quick_detect(df: pd.DataFrame) -> Dict:
    """
    Fonction simplifiée pour une détection rapide.
    Usage: from schema_detector import quick_detect
           result = quick_detect(df)
    """
    detector = ColumnDetector()
    return detector.detect_schema(df)


def format_detection_results(schema: Dict) -> pd.DataFrame:
    """
    Convertit les résultats de détection en DataFrame lisible.
    """
    rows = []
    for col_name, meta in schema.items():
        rows.append({
            'Colonne Originale': col_name,
            'Type Détecté': meta['detected_type'],
            'Type Technique': meta['technical_type'],
            'Confiance': f"{meta['confidence']:.0%}",
            'Nom Suggéré': meta['suggested_name'],
            'Valeurs Uniques': meta['unique_count'],
            'Valeurs Manquantes': meta['null_count']
        })
    
    return pd.DataFrame(rows)
