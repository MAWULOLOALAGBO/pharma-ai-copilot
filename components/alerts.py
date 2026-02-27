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
        for col, meta in self.schema.items():
            if meta['detected_type'] == 'date':
                if any(x in col.lower() for x in ['peremption', 'expiration', 'dlc', 'dluo']):
                    return col
        # Fallback sur première date trouvée
        for col, meta in self.schema.items():
            if meta['detected_type'] == 'date':
                return col
        return None
    
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
        for col, meta in self.schema.items():
            if meta['detected_type'] == 'price':
                return col
        return None
    
    def _find_product_column(self) -> str:
        """Trouve la colonne de nom de produit."""
        for col, meta in self.schema.items():
            if meta['detected_type'] == 'product':
                return col
        return None
    
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
            prix = pd.to_numeric(self.df[self.price_col], errors='coerce')
            qte = pd.to_numeric(self.df[self.qty_col], errors='coerce') if self.qty_col else pd.Series([1] * len(self.df))
            masque_perime = jours_restant < 0
            valeur_pertes = (prix * qte * masque_perime).sum()
        
        # Liste détaillée des produits critiques
        produits_critiques = []
        if self.product_col:
            masque_critique = (jours_restant >= 0) & (jours_restant <= self.SEUIL_PEREMPTION_ATTENTION)
            indices = self.df[masque_critique].index
            
            for idx in indices[:10]:  # Top 10
                produits_critiques.append({
                    'produit': self.df.loc[idx, self.product_col],
                    'date_peremption': dates.loc[idx].strftime('%d/%m/%Y') if pd.notna(dates.loc[idx]) else 'N/A',
                    'jours_restant': int(jours_restant.loc[idx]),
                    'quantite': self.df.loc[idx, self.qty_col] if self.qty_col else 'N/A',
                    'priorite': 'URGENT' if jours_restant.loc[idx] <= self.SEUIL_PEREMPTION_URGENT else 'ATTENTION'
                })
        
        return {
            'total_produits': len(self.df),
            'perimes': int(perimes),
            'urgent_30j': int(urgent),
            'attention_60j': int(attention),
            'avis_90j': int(avis),
            'ok': int(ok),
            'valeur_pertes_estimee': valeur_pertes,
            'produits_prioritaires': produits_critiques,
            'taux_rotation_risque': (perimes + urgent) / len(self.df) * 100 if len(self.df) > 0 else 0
        }
    
    def calculate_stock_alerts(self) -> Dict[str, Any]:
        """
        Calcule les alertes de rupture et surstock.
        """
        if not self.qty_col:
            return {'error': 'Aucune colonne de quantité détectée'}
        
        qty = pd.to_numeric(self.df[self.qty_col], errors='coerce')
        
        # Recherche d'une colonne stock_min
        stock_min_col = None
        for col in self.df.columns:
            if 'min' in col.lower() and ('stock' in col.lower() or 'seuil' in col.lower()):
                stock_min_col = col
                break
        
        # Calcul des alertes
        if stock_min_col:
            stock_min = pd.to_numeric(self.df[stock_min_col], errors='coerce')
            ruptures = (qty <= 0).sum()
            sous_seuil = ((qty > 0) & (qty < stock_min)).sum()
        else:
            ruptures = (qty <= 0).sum()
            sous_seuil = (qty <= 5).sum()  # Seuil par défaut
        
        # Surstock (quantité très élevée)
        q75 = qty.quantile(0.75)
        surstock = (qty > q75 * 3).sum()
        
        return {
            'ruptures': int(ruptures),
            'sous_seuil': int(sous_seuil),
            'surstock': int(surstock),
            'stock_total': int(qty.sum()),
            'stock_moyen': float(qty.mean()),
            'produits_dormants': int((qty > 0).sum() - ruptures - sous_seuil)  # Stock OK mais pas mouvementé
        }
    
    def calculate_margin_alerts(self) -> Dict[str, Any]:
        """
        Calcule les alertes sur les marges et prix.
        """
        # Recherche des colonnes prix achat et prix vente
        prix_achat_col = None
        prix_vente_col = None
        
        for col, meta in self.schema.items():
            if meta['detected_type'] == 'price':
                if any(x in col.lower() for x in ['achat', 'pa', 'ht']):
                    prix_achat_col = col
                elif any(x in col.lower() for x in ['vente', 'pv', 'ttc', 'public']):
                    prix_vente_col = col
        
        if not prix_achat_col or not prix_vente_col:
            return {'error': 'Colonnes prix achat et/ou vente non détectées'}
        
        # Nettoyage des prix
        pa = pd.to_numeric(self.df[prix_achat_col], errors='coerce')
        pv = pd.to_numeric(self.df[prix_vente_col], errors='coerce')
        
        # Calcul de la marge
        marge = ((pv - pa) / pa * 100).replace([np.inf, -np.inf], np.nan)
        marge_brute = (pv - pa).sum()
        
        # Anomalies
        prix_vente_trop_bas = (pv < pa).sum()  # Vente à perte
        marge_negative = (marge < 0).sum()
        marge_faible = ((marge >= 0) & (marge < 10)).sum()  # Marge < 10%
        marge_elevee = (marge > 100).sum()  # Marge > 100%
        
        return {
            'marge_moyenne_pct': float(marge.mean()),
            'marge_brute_totale': float(marge_brute),
            'prix_vente_trop_bas': int(prix_vente_trop_bas),
            'marge_negative': int(marge_negative),
            'marge_faible': int(marge_faible),
            'marge_elevee': int(marge_elevee),
            'prix_moyen_achat': float(pa.mean()),
            'prix_moyen_vente': float(pv.mean())
        }
    
    def get_all_alerts(self) -> Dict[str, Any]:
        """
        Retourne toutes les alertes consolidées.
        """
        return {
            'fefo': self.calculate_fefo_alerts(),
            'stock': self.calculate_stock_alerts(),
            'marge': self.calculate_margin_alerts(),
            'timestamp': datetime.now().isoformat()
        }


# Fonction utilitaire
def generate_pharma_alerts(df: pd.DataFrame, schema: Dict) -> Dict[str, Any]:
    """
    Génère toutes les alertes pharmaceutiques rapidement.
    """
    alerts = PharmaAlerts(df, schema)
    return alerts.get_all_alerts()
