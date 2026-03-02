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
                jours_restants_val = int(row['__jours_restant'])
                
                # Détermination de la priorité avec seuils clairs
                if jours_restants_val <= self.SEUIL_PEREMPTION_URGENT:
                    priorite = 'URGENT'
                    priorite_desc = f'{jours_restants_val} jours (≤30)'
                elif jours_restants_val <= self.SEUIL_PEREMPTION_ATTENTION:
                    priorite = 'ATTENTION'
                    priorite_desc = f'{jours_restants_val} jours (31-60)'
                else:
                    priorite = 'AVIS'
                    priorite_desc = f'{jours_restants_val} jours (61-90)'
                
                # Récupération de la quantité avec gestion d'erreur améliorée
                try:
                    qty_val = pd.to_numeric(row[self.qty_col], errors='coerce')
                    if pd.isna(qty_val):
                        qty_display = 'N/A'
                    else:
                        qty_display = int(qty_val)
                except Exception as e:
                    qty_display = 'N/A'
                
                # Récupération de la date formatée
                date_val = dates.loc[idx] if idx in dates.index else None
                if pd.notna(date_val):
                    date_str = date_val.strftime('%d/%m/%Y')
                else:
                    date_str = 'N/A'
                
                produits_critiques.append({
                    'produit': row[self.product_col],
                    'date_peremption': date_str,
                    'jours_restant': jours_restants_val,
                    'jours_restant_desc': priorite_desc,
                    'quantite': qty_display,
                    'priorite': priorite
                })
            
            # Nettoyage colonne temporaire
            if '__jours_restant' in self.df.columns:
                self.df.drop(columns=['__jours_restant'], inplace=True, errors='ignore')
        
        return {
            'total_produits': len(self.df),
            'perimes': int(perimes),
            'urgent_30j': int(urgent),
            'attention_60j': int(attention),
            'avis_90j': int(avis),
            'ok': int(ok),
            'valeur_pertes_estimee': round(valeur_pertes, 2),
            'produits_prioritaires': produits_critiques,
            'taux_rotation_risque': round((perimes + urgent) / len(self.df) * 100, 1) if len(self.df) > 0 else 0,
            'date_analyse': datetime.now().strftime('%d/%m/%Y %H:%M')
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
        q95 = qty.quantile(0.95)
        surstock = (qty > q95).sum() if not pd.isna(q95) else 0
        
        # Produits dormants (stock OK mais pas mouvementé - approximation)
        produits_dormants = ((qty > 5) & (qty <= q75)).sum()
        
        return {
            'ruptures': int(ruptures),
            'sous_seuil': int(sous_seuil),
            'surstock': int(surstock),
            'stock_total': int(qty.sum()),
            'stock_moyen': round(float(qty.mean()), 1),
            'stock_median': round(float(qty.median()), 1),
            'produits_dormants': int(produits_dormants),
            'qty_col_used': self.qty_col  # Debug: colonne utilisée
        }
    
    def calculate_margin_alerts(self) -> Dict[str, Any]:
        """
        Calcule les alertes sur les marges et prix.
        """
        # Recherche des colonnes prix achat et prix vente
        prix_achat_col = None
        prix_vente_col = None
        
        for col in self.price_cols:
            col_lower = col.lower()
            if any(x in col_lower for x in ['achat', 'pa', 'ht', 'revient']):
                prix_achat_col = col
            elif any(x in col_lower for x in ['vente', 'pv', 'ttc', 'public', 'client']):
                prix_vente_col = col
        
        # Fallback si pas trouvé
        if not prix_achat_col and len(self.price_cols) >= 2:
            prix_achat_col = self.price_cols[0]  # Premier = achat généralement
            prix_vente_col = self.price_cols[1]  # Deuxième = vente
        elif not prix_vente_col and len(self.price_cols) == 1:
            return {'error': 'Besoin de 2 colonnes prix (achat et vente)'}
        
        if not prix_achat_col or not prix_vente_col:
            return {'error': f'Colonnes prix achat ({prix_achat_col}) et/ou vente ({prix_vente_col}) non détectées'}
        
        # Nettoyage des prix
        pa = self._clean_prices(self.df[prix_achat_col])
        pv = self._clean_prices(self.df[prix_vente_col])
        
        # Filtrer les valeurs valides
        valid_mask = pa.notna() & pv.notna() & (pa > 0)
        pa_valid = pa[valid_mask]
        pv_valid = pv[valid_mask]
        
        if len(pa_valid) == 0:
            return {'error': 'Aucun prix valide pour calculer les marges'}
        
        # Calcul de la marge
        marge_pct = ((pv_valid - pa_valid) / pa_valid * 100)
        marge_brute = (pv_valid - pa_valid).sum()
        marge_brute_totale = (pv - pa).sum()
        
        # Anomalies
        prix_vente_trop_bas = (pv < pa).sum()
        marge_negative = (marge_pct < 0).sum()
        marge_faible = ((marge_pct >= 0) & (marge_pct < 10)).sum()
        marge_elevee = (marge_pct > 100).sum()
        marge_optimale = ((marge_pct >= 30) & (marge_pct <= 50)).sum()
        
        # Top produits avec marge faible
        produits_marge_faible = []
        if self.product_col and marge_faible > 0:
            marge_mask = (marge_pct >= 0) & (marge_pct < 10)
            df_marge = self.df[valid_mask & marge_mask].copy()
            df_marge['__marge_pct'] = marge_pct[marge_mask]
            df_marge = df_marge.sort_values('__marge_pct')
            
            for idx, row in df_marge.head(5).iterrows():
                produits_marge_faible.append({
                    'produit': row[self.product_col],
                    'prix_achat': round(pa.loc[idx], 2),
                    'prix_vente': round(pv.loc[idx], 2),
                    'marge_pct': round(marge_pct.loc[idx], 1)
                })
        
        return {
            'marge_moyenne_pct': round(float(marge_pct.mean()), 1),
            'marge_mediane_pct': round(float(marge_pct.median()), 1),
            'marge_brute_totale': round(float(marge_brute_totale), 2),
            'prix_moyen_achat': round(float(pa_valid.mean()), 2),
            'prix_moyen_vente': round(float(pv_valid.mean()), 2),
            'prix_vente_trop_bas': int(prix_vente_trop_bas),
            'marge_negative': int(marge_negative),
            'marge_faible': int(marge_faible),
            'marge_elevee': int(marge_elevee),
            'marge_optimale': int(marge_optimale),
            'produits_marge_faible': produits_marge_faible,
            'colonne_achat': prix_achat_col,
            'colonne_vente': prix_vente_col
        }
    
    def get_all_alerts(self) -> Dict[str, Any]:
        """
        Retourne toutes les alertes consolidées.
        """
        return {
            'fefo': self.calculate_fefo_alerts(),
            'stock': self.calculate_stock_alerts(),
            'marge': self.calculate_margin_alerts(),
            'timestamp': datetime.now().isoformat(),
            'version': '4.1.1'
        }


# Fonction utilitaire
def generate_pharma_alerts(df: pd.DataFrame, schema: Dict) -> Dict[str, Any]:
    """
    Génère toutes les alertes pharmaceutiques rapidement.
    """
    alerts = PharmaAlerts(df, schema)
    return alerts.get_all_alerts()
