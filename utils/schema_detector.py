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
        Retourne un dict {categorie: score}
        """
        scores = {}
        
        # Fonction helper pour calculer le score d'une catégorie
        def calc_score(keywords, weight=1.0):
            score = 0.0
            for keyword in keywords:
                if keyword in name:
                    # Score plus élevé si correspondance exacte ou au début
                    if name == keyword:
                        score += 1.0 * weight
                    elif name.startswith(keyword):
                        score += 0.8 * weight
                    else:
                        score += 0.5 * weight
            return min(score, 1.0)  # Plafond à 1.0
        
        scores['product'] = calc_score(self.product_keywords)
        scores['code'] = calc_score(self.code_keywords)
        scores['quantity'] = calc_score(self.quantity_keywords)
        scores['price'] = calc_score(self.price_keywords)
        scores['date'] = calc_score(self.date_keywords)
        scores['category'] = calc_score(self.category_keywords)
        scores['brand'] = calc_score(self.brand_keywords)
        
        return scores
    
    def _score_column_values(self, col_data: pd.Series) -> Tuple[Dict[str, float], str]:
        """
        Analyse les valeurs d'une colonne pour déterminer leur type.
        Retourne (scores par catégorie, type_technique)
        """
        scores = {}
        
        # Suppression des valeurs nulles pour analyse
        values = col_data.dropna().astype(str)
        total_values = len(values)
        
        if total_values == 0:
            return {}, 'empty'
        
        # Détection du type technique pandas
        dtype = col_data.dtype
        
        if pd.api.types.is_datetime64_any_dtype(col_data):
            technical_type = 'date'
        elif pd.api.types.is_integer_dtype(col_data):
            technical_type = 'int'
        elif pd.api.types.is_float_dtype(col_data):
            technical_type = 'float'
        elif pd.api.types.is_bool_dtype(col_data):
            technical_type = 'bool'
        else:
            technical_type = 'string'
        
        # Analyse des patterns dans les valeurs
        cip_matches = values.str.match(self.cip_pattern).sum()
        ean_matches = values.str.match(self.ean_pattern).sum()
        date_matches = sum(values.str.match(pattern).sum() for pattern in self.date_patterns)
        price_matches = values.str.match(self.price_pattern).sum()
        quantity_matches = values.str.match(self.quantity_pattern).sum()
        url_matches = values.str.match(self.url_pattern).sum()
        
        # Calcul des pourcentages de correspondance
        cip_ratio = cip_matches / total_values
        ean_ratio = ean_matches / total_values
        date_ratio = date_matches / total_values
        price_ratio = price_matches / total_values
        quantity_ratio = quantity_matches / total_values
        url_ratio = url_matches / total_values
        
        # Attribution des scores
        # Code: CIP ou EAN ou format code-like (chiffres, tirets)
        scores['code'] = max(cip_ratio, ean_ratio, 0.3 if technical_type == 'string' and values.str.len().mean() < 20 else 0)
        
        # Date: pattern date détecté ou type datetime
        scores['date'] = max(date_ratio, 1.0 if technical_type == 'date' else 0)
        
        # Quantité: entiers positifs, ou colonne numérique avec valeurs raisonnables
        if technical_type in ['int', 'float']:
            # Vérifie si les valeurs sont des entiers positifs raisonnables (stock)
            numeric_values = pd.to_numeric(col_data.dropna(), errors='coerce')
            if (numeric_values >= 0).all() and (numeric_values <= 100000).all():
                scores['quantity'] = 0.9 if quantity_ratio > 0.8 else 0.6
            else:
                scores['quantity'] = 0.3
        else:
            scores['quantity'] = quantity_ratio
        
        # Prix: pattern prix ou valeurs décimales typiques
        if technical_type == 'float' or price_ratio > 0.5:
            scores['price'] = 0.9
        else:
            scores['price'] = price_ratio
        
        # Produit: texte long, variété importante, pas de pattern spécifique
        if technical_type == 'string':
            avg_length = values.str.len().mean()
            unique_ratio = col_data.nunique() / total_values
            
            # Nom de produit = texte moyen/long, beaucoup d'unicité
            if 10 < avg_length < 200 and unique_ratio > 0.5:
                scores['product'] = 0.8
            else:
                scores['product'] = 0.3
        else:
            scores['product'] = 0.0
        
        # Catégorie: texte court, valeurs répétées
        if technical_type == 'string':
            unique_ratio = col_data.nunique() / total_values
            avg_length = values.str.len().mean()
            
            # Catégorie = texte court, peu d'unicité (catégories récurrentes)
            if avg_length < 50 and unique_ratio < 0.3:
                scores['category'] = 0.8
            else:
                scores['category'] = 0.2
        else:
            scores['category'] = 0.0
        
        # Marque: texte, valeurs répétées, mots communs de marques
        if technical_type == 'string':
            # Détection de marques connues dans les valeurs
            brand_indicators = ['siemens', 'kimo', 'wago', 'jumo', 'pharma', 'med', 'bio', 'lab']
            brand_matches = values.str.lower().str.contains('|'.join(brand_indicators)).sum()
            brand_ratio = brand_matches / total_values
            
            scores['brand'] = brand_ratio if brand_ratio > 0.3 else 0.2 if unique_ratio < 0.4 else 0.0
        else:
            scores['brand'] = 0.0
        
        # URL: pattern http détecté
        scores['url'] = url_ratio
        
        return scores, technical_type
    
    def _get_standardized_name(self, detected_type: str, original_name: str) -> str:
        """
        Retourne un nom standardisé selon le type détecté.
        """
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
