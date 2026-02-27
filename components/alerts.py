"""
=============================================================================
MODULE : alerts.py
=============================================================================

DESCRIPTION :
-------------
Gestion des alertes pharmaceutiques critiques :
- Péremption (FEFO/FIFO)
- Ruptures de stock
- Anomalies de prix
- Stupéfiants (à venir)

AUTEUR : Mawulolo Koffi Parfait ALAGBO
DATE : 2025-02-27
=============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple


class PharmaAlerts:
    """
    Détecteur d'alertes pharmaceutiques critiques.
    """
    
    def __init__(self, df: pd.DataFrame, schema: Dict):
        """
        Initialise avec les données et le schéma détecté.
        """
        self.df = df.copy()
        self.schema = schema
        
        # EXTRAIRE les colonnes par type (CORRECTION BUG)
        self.quantity_cols = [c for c, m in schema.items() if m['detected_type'] == 'quantity']
        self.price_cols = [c for c, m in schema.items() if m['detected_type'] == 'price']
        self.product_cols = [c for c, m in schema.items() if m['detected_type'] == 'product']
        self.date_cols = [c for c, m in schema.items() if m['detected_type'] == 'date']
        self.brand_cols = [c for c, m in schema.items() if m['detected_type'] == 'brand']
        self.category_cols = [c for c, m in schema.items() if m['detected_type'] == 'category']
        self.code_cols = [c for c, m in schema.items() if m['detected_type'] == 'code']
        
        # Identification des colonnes clés
        self.date_peremption = self._find_date_column()
        self.qty_col = self._find_quantity_column()
        self.price_col = self._find_price_column()
        self.product_col = self._find_product_column()
        
        # Seuils d'alerte
        self.SEUIL_PEREMPTION_URGENT = 30   # jours
        self.SEUIL_PEREMPTION_ATTENTION = 60  # jours
        self.SEUIL_PEREMPTION_AVIS = 90     # jours
        self.SEUIL_RUPTURE = 0              # unités
    
    def _find_date_column(self) -> str:
        """Trouve la colonne de date de péremption."""
        # Utilise self.date_cols maintenant défini
        for col in self.date_cols:
            if any(x in col.lower() for x in ['peremption', 'expiration', 'dlc', 'dluo']):
                return col
        # Fallback sur première date trouvée
        return self.date_cols[0] if self.date_cols else None
    
    def _find_quantity_column(self) -> str:
        """
        Trouve la meilleure colonne de quantité en stock.
        Priorité : stock_actuel > stock_physique > quantité > autres
        """
        if not self.quantity_cols:
            return None
        
        # Priorité 1 : stock actuel/physique/réel
        for col in self.quantity_cols:
            clean = col.lower().replace('_', '').replace(' ', '')
            if any(x in clean for x in ['actuel', 'physique', 'reel', 'real', 'courant', 'current']):
                return col
        
        # Priorité 2 : stock sans min/max/seuil
        for col in self.quantity_cols:
            clean = col.lower().replace('_', '').replace(' ', '')
            if 'stock' in clean and not any(x in clean for x in ['min', 'max', 'seuil', 'alerte', 'minimum', 'maximum', 'secu', 'securite']):
                return col
        
        # Priorité 3 : quantité/qty/qte
        for col in self.quantity_cols:
            clean = col.lower().replace('_', '').replace(' ', '')
            if any(x in clean for x in ['quantite', 'quantity', 'qty', 'qte']):
                return col
        
        # Fallback
        return self.quantity_cols[0]
    
    def _find_price_column(self) -> str:
        """Trouve la colonne de prix."""
        # Priorité : prix de vente TTC > prix d'achat
        for col in self.price_cols:
            if any(x in col.lower() for x in ['vente', 'pv', 'ttc', 'public']):
                return col
        return self.price_cols[0] if self.price_cols else None
    
    def _find_product_column(self) -> str:
        """Trouve la colonne de nom de produit."""
        # Priorité : designation > libelle > produit > nom
        for col in self.product_cols:
            clean = col.lower().replace('_', '')
            if 'designation' in clean:
                return col
        for col in self.product_cols:
            clean = col.lower().replace('_', '')
            if 'libelle' in clean or 'libellé' in clean:
                return col
        return self.product_cols[0] if self.product_cols else None
    
    def _parse_dates(self, series: pd.Series) -> pd.Series:
        """
        Parse les dates (Excel, string, datetime).
        """
        # Si déjà datetime
        if pd.api.types.is_datetime64_any_dtype(series):
            return series
        
        # Si nombres (Excel)
        try:
            numeric = pd.to_numeric(series, errors='coerce')
            if numeric.notna().sum() > 0:
                # Conversion Excel date
                from datetime import datetime as dt, timedelta
                excel_epoch = dt(1899, 12, 30)
                return numeric.dropna().apply(lambda x: excel_epoch + timedelta(days=int(x)))
        except:
            pass
        
        # Si strings
        return pd.to_datetime(series, errors='coerce', dayfirst=True)
    
    def _clean_prices(self, series: pd.Series) -> pd.Series:
        """
        Nettoie une série de prix (supprime €, espaces, convertit virgules).
        """
        prices = series.astype(str)
        prices = prices.str.replace('€', '', regex=False)
        prices = prices.str.replace('EUR', '', regex=False, case=False)
        prices = prices.str.replace(' ', '', regex=False)
        prices = prices.str.replace('\xa0', '', regex=False)
        prices = prices.str.replace(',', '.', regex=False)
        prices = prices.str.strip()
        return pd.to_numeric(prices, errors='coerce')
    
    def calculate_fefo_alerts(self) -> Dict[str, Any]:
        """
        Calcule les alertes FEFO (First Expired First Out).
        
        Returns:
            dict avec les statistiques de péremption
        """
        if not self.date_peremption:
            return {'error': 'Aucune colonne de date de péremption détectée'}
        
        # Conversion des dates
        dates = self._parse_dates(self.df[self.date_peremption])
        today = datetime.now()
        
        # Calcul des jours restants
        jours_restant = (dates - today).dt.days
        
        # Classification
        perimes = (jours_restant < 0).sum()
        urgent = ((jours_restant >= 0) & (jours_restant <= self.SEUIL_PEREMPTION_URGENT)).sum()
        attention = ((jours_restant > self.SEUIL_PEREMPTION_URGENT) & 
                     (jours_restant <= self.SEUIL_PEREMPTION_ATTENTION)).sum()
        avis = ((jours_restant > self.SEUIL_PEREMPTION_ATTENTION) & 
                (jours_restant <= self.SEUIL_PEREMPTION_AVIS)).sum()
        ok = (jours_restant > self.SEUIL_PEREMPTION_AVIS).sum()
        
        # Valeur des pertes (produits périmés)
        valeur_pertes = 0
        if self.price_col and perimes > 0:
            prix = self._clean_prices(self.df[self.price_col])
            qte = pd.to_numeric(self.df[self.qty_col], errors='coerce') if self.qty_col else pd.Series([1] * len(self.df))
            masque_perime = jours_restant < 0
            valeur_pertes = (prix * qte * masque_perime).sum()
        
        # Liste détaillée des produits critiques (CORRECTION BUG)
        produits_critiques = []
        if self.product_col and self.qty_col:
            # Masque : produits non périmés mais à risque (< 60 jours)
            masque_critique = (jours_restant >= 0) & (jours_restant <= self.SEUIL_PEREMPTION_ATTENTION)
            df_critique = self.df[masque_critique].copy()
            
            # Trier par jours restants (les plus urgents d'abord)
            df_critique['__jours_restant'] = jours_restant[masque_critique]
            df_critique = df_critique.sort_values('__jours_restant')
            
            for idx, row in df_critique.head(10).iterrows():
                jours_restants_val =
