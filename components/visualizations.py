"""
=============================================================================
MODULE : visualizations.py
=============================================================================

DESCRIPTION :
-------------
Générateur automatique de visualisations basé sur la détection de schéma.
Ce module crée des graphiques pertinents sans configuration manuelle.

LOGIQUE :
---------
- Si colonne 'brand' détectée → Graphique répartition par marque
- Si colonne 'category' détectée → Graphique répartition par catégorie
- Si colonne 'price' détectée → Histogramme des prix, top 10 plus chers
- Si colonne 'quantity' détectée → Indicateurs de stock, alertes
- Si colonne 'product' + 'price' → Scatter plot prix vs produits

AUTEUR : Mawulolo Koffi Parfait ALAGBO
DATE : 2025-02-25
=============================================================================
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from typing import Dict, List, Any, Optional


class AutoVizGenerator:
    """
    Générateur automatique de visualisations intelligentes.
    """
    
    def __init__(self, df: pd.DataFrame, schema: Dict):
        """
        Initialise le générateur avec les données et le schéma détecté.
        
        Args:
            df (pd.DataFrame): Les données
            schema (dict): Résultat de la détection de schéma
        """
        self.df = df
        self.schema = schema
        
        # Extraction des colonnes par type pour faciliter l'accès
        self.product_cols = self._get_cols_by_type('product')
        self.code_cols = self._get_cols_by_type('code')
        self.quantity_cols = self._get_cols_by_type('quantity')
        self.price_cols = self._get_cols_by_type('price')
        self.date_cols = self._get_cols_by_type('date')
        self.category_cols = self._get_cols_by_type('category')
        self.brand_cols = self._get_cols_by_type('brand')
    
    def _get_cols_by_type(self, detected_type: str) -> List[str]:
        """
        Retourne la liste des colonnes d'un type détecté spécifique.
        """
        return [
            col for col, meta in self.schema.items() 
            if meta['detected_type'] == detected_type
        ]
    
    def generate_all_visualizations(self) -> Dict[str, Any]:
        """
        Génère toutes les visualisations pertinentes selon le schéma.
        
        Returns:
            dict: {'nom_graphique': figure_plotly, ...}
        """
        figures = {}
        
        # 1. Répartition par marque (si disponible)
        if self.brand_cols:
            figures['repartition_marques'] = self._create_brand_chart()
        
        # 2. Répartition par catégorie (si disponible)
        if self.category_cols:
            figures['repartition_categories'] = self._create_category_chart()
        
        # 3. Analyse des prix (si disponible)
        if self.price_cols:
            figures['distribution_prix'] = self._create_price_distribution()
            figures['top_produits_chers'] = self._create_top_expensive()
        
        # 4. Analyse des quantités (si disponible)
        if self.quantity_cols:
            figures['indicateurs_stock'] = self._create_stock_indicators()
        
        # 5. Matrice produit-prix (si les deux disponibles)
        if self.product_cols and self.price_cols:
            figures['vue_produit_prix'] = self._create_product_price_view()
        
        # 6. Tableau de bord synthétique (toujours créé)
        figures['kpi_cards'] = self._create_kpi_cards()
        
        return figures
    
    def _create_brand_chart(self) -> go.Figure:
        """
        Crée un graphique de répartition par marque (Pie ou Bar).
        """
        brand_col = self.brand_cols[0]  # Prend la première colonne marque
        
        # Compte les occurrences par marque
        brand_counts = self.df[brand_col].value_counts().head(10)  # Top 10
        
        # Choix du type de graphique selon le nombre de marques
        if len(brand_counts) <= 6:
            # Pie chart pour peu de catégories
            fig = px.pie(
                values=brand_counts.values,
                names=brand_counts.index,
                title=f"📊 Répartition par {brand_col}",
                color_discrete_sequence=px.colors.sequential.Blues_r
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
        else:
            # Bar chart pour beaucoup de catégories
            fig = px.bar(
                x=brand_counts.index,
                y=brand_counts.values,
                title=f"📊 Top 10 des {brand_col}",
                labels={'x': brand_col, 'y': 'Nombre de produits'},
                color=brand_counts.values,
                color_continuous_scale='Blues'
            )
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=12),
            title_font_size=16,
            title_font_color='#1e3a8a'
        )
        
        return fig
    
    def _create_category_chart(self) -> go.Figure:
        """
        Crée un graphique de répartition par catégorie.
        """
        # Si plusieurs colonnes catégorie, on prend celle avec le meilleur score
        category_col = self.category_cols[0]
        
        # Pour les catégories, on fait un treemap si imbriqué possible
        cat_counts = self.df[category_col].value_counts().head(15)
        
        fig = px.bar(
            y=cat_counts.index[::-1],  # Inversé pour meilleure lisibilité
            x=cat_counts.values[::-1],
            orientation='h',
            title=f"📂 Répartition par {category_col}",
            labels={'y': category_col, 'x': 'Nombre de produits'},
            color=cat_counts.values[::-1],
            color_continuous_scale='Greens',
            text=cat_counts.values[::-1]
        )
        
        fig.update_traces(textposition='outside')
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(autorange="reversed"),
            title_font_size=16,
            title_font_color='#1e3a8a',
            height=400 if len(cat_counts) > 8 else 300
        )
        
        return fig
    
    def _clean_price_series(self, price_col: str) -> pd.Series:
        """
        Nettoie une série de prix (gère €, espaces, virgules, etc.).
        
        Args:
            price_col (str): Nom de la colonne prix
            
        Returns:
            pd.Series: Série de prix numériques nettoyés
        """
        # Copie des données
        prices = self.df[price_col].astype(str)
        
        # Nettoyage : suppression des symboles €, espaces, remplacement virgule par point
        prices_clean = prices.str.replace('€', '', regex=False)
        prices_clean = prices_clean.str.replace('EUR', '', regex=False, case=False)
        prices_clean = prices_clean.str.replace(' ', '', regex=False)
        prices_clean = prices_clean.str.replace(',', '.', regex=False)
        prices_clean = prices_clean.str.replace('\xa0', '', regex=False)  # Espace insécable
        prices_clean = prices_clean.str.strip()
        
        # Conversion en numérique
        prices_numeric = pd.to_numeric(prices_clean, errors='coerce')
        
        return prices_numeric
    
    def _create_price_distribution(self) -> go.Figure:
        """
        Crée un histogramme de distribution des prix.
        """
        price_col = self.price_cols[0]
        
        # Utilisation de la fonction de nettoyage
        prices = self._clean_price_series(price_col).dropna()
        
        if len(prices) == 0:
            # Fallback si conversion échoue
            fig = go.Figure()
            fig.add_annotation(
                text="Impossible d'analyser les prix (format non numérique)",
                xref="paper", yref="paper",
                showarrow=False, font=dict(size=14, color="red")
            )
            return fig
        
        fig = px.histogram(
            prices,
            nbins=20,
            title=f"💰 Distribution des prix ({price_col})",
            labels={'value': 'Prix (€)', 'count': 'Nombre de produits'},
            color_discrete_sequence=['#3b82f6']
        )
        
        # Ajout de statistiques
        mean_price = prices.mean()
        median_price = prices.median()
        
        fig.add_vline(
            x=mean_price, 
            line_dash="dash", 
            line_color="red",
            annotation_text=f"Moy: {mean_price:.2f}€"
        )
        
        fig.add_vline(
            x=median_price, 
            line_dash="dash", 
            line_color="green",
            annotation_text=f"Méd: {median_price:.2f}€"
        )
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            title_font_size=16,
            title_font_color='#1e3a8a',
            showlegend=False,
            bargap=0.1
        )
        
        return fig
    
    def _create_top_expensive(self) -> go.Figure:
        """
        Crée un graphique des produits les plus chers.
        """
        price_col = self.price_cols[0]
        product_col = self.product_cols[0] if self.product_cols else None
        
        # Nettoyage des prix
        self.df['_price_num'] = self._clean_price_series(price_col)
        
        # Filtrage des valeurs valides
        valid_df = self.df.dropna(subset=['_price_num'])
        
        if len(valid_df) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="Aucun prix valide trouvé",
                xref="paper", yref="paper",
                showarrow=False, font=dict(size=14, color="red")
            )
            return fig
        
        # Top 10 plus chers
        top_expensive = valid_df.nlargest(10, '_price_num')
        
        # Nom des produits (tronqué si trop long)
        if product_col:
            labels = top_expensive[product_col].astype(str).str[:30] + "..."
        else:
            labels = top_expensive.index.astype(str)
        
        fig = px.bar(
            x=top_expensive['_price_num'],
            y=labels,
            orientation='h',
            title=f"🏆 Top 10 des produits les plus chers",
            labels={'x': 'Prix (€)', 'y': 'Produit'},
            color=top_expensive['_price_num'],
            color_continuous_scale='Reds',
            text=top_expensive['_price_num'].apply(lambda x: f"{x:,.2f}€")
        )
        
        fig.update_traces(textposition='outside')
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(autorange="reversed"),
            title_font_size=16,
            title_font_color='#1e3a8a',
            height=500
        )
        
        # Nettoyage colonne temporaire
        self.df.drop(columns=['_price_num'], inplace=True, errors='ignore')
        
        return fig
    
    def _create_stock_indicators(self) -> go.Figure:
        """
        Crée des indicateurs de stock avec meilleur contraste.
        """
        quantity_col = self.quantity_cols[0]
        
        # Conversion en numérique
        quantities = pd.to_numeric(self.df[quantity_col], errors='coerce').dropna()
        
        if len(quantities) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="Impossible d'analyser les quantités",
                xref="paper", yref="paper",
                showarrow=False, font=dict(size=14, color="red")
            )
            return fig
        
        # Calcul des stats
        total = int(quantities.sum())
        avg = round(quantities.mean(), 1)
        max_val = int(quantities.max())
        min_val = int(quantities.min())
        
        # Création de la figure avec 4 indicateurs
        fig = go.Figure()
        
        # On utilise une grille 2x2 avec des annotations pour les titres
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=total,
                title={"text": "<b style='color:#1e40af;'>Stock Total</b>", "font": {"size": 16}},
                number={
                    'suffix': " unités", 
                    'font': {'size': 40, 'color': '#1e40af'},
                    'valueformat': ',d'
                },
                domain={'x': [0, 0.5], 'y': [0.5, 1]}
            )
        )
        
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=avg,
                title={"text": "<b style='color:#10b981;'>Moyenne par Produit</b>", "font": {"size": 16}},
                number={
                    'suffix': " unités", 
                    'font': {'size': 40, 'color': '#10b981'}
                },
                domain={'x': [0.5, 1], 'y': [0.5, 1]}
            )
        )
        
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=max_val,
                title={"text": "<b style='color:#f59e0b;'>Produit Max</b>", "font": {"size": 16}},
                number={
                    'suffix': " unités", 
                    'font': {'size': 40, 'color': '#f59e0b'},
                    'valueformat': ',d'
                },
                domain={'x': [0, 0.5], 'y': [0, 0.5]}
            )
        )
        
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=min_val,
                title={"text": "<b style='color:#ef4444;'>Produit Min</b>", "font": {"size": 16}},
                number={
                    'suffix': " unités", 
                    'font': {'size': 40, 'color': '#ef4444'},
                    'valueformat': ',d'
                },
                domain={'x': [0.5, 1], 'y': [0, 0.5]}
            )
        )
        
        fig.update_layout(
            title_text="📦 Indicateurs de Stock",
            title_font_size=20,
            title_font_color='#1e3a8a',
            paper_bgcolor='white',
            height=400,
            margin=dict(t=80, b=20, l=20, r=20)
        )
        
        return fig
    
    def _create_product_price_view(self) -> go.Figure:
        """
        Crée une vue combinée produit-prix (scatter plot amélioré).
        """
        price_col = self.price_cols[0]
        product_col = self.product_cols[0]
        
        # Préparation des données
        plot_df = self.df.copy()
        plot_df['_price_num'] = self._clean_price_series(price_col)
        
        # Filtrage des valeurs valides
        plot_df = plot_df.dropna(subset=['_price_num'])
        
        if len(plot_df) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="Aucune donnée prix/produit valide",
                xref="paper", yref="paper",
                showarrow=False, font=dict(size=14, color="red")
            )
            return fig
        
        # Tronquer les noms de produits pour lisibilité
        plot_df['product_short'] = plot_df[product_col].astype(str).str[:25] + "..."
        
        # Ajout couleur par marque si disponible
        color_col = self.brand_cols[0] if self.brand_cols else None
        
        fig = px.scatter(
            plot_df,
            x='product_short',
            y='_price_num',
            color=color_col if color_col else None,
            title=f"💎 Vue Produits vs Prix",
            labels={
                'product_short': 'Produit',
                '_price_num': 'Prix (€)'
            },
            hover_data=[product_col, price_col] if price_col != '_price_num' else [product_col],
            size='_price_num',
            size_max=25,
            color_discrete_sequence=px.colors.qualitative.Bold if not color_col else None
        )
        
        # Amélioration de la lisibilité
        fig.update_traces(
            marker=dict(
                opacity=0.8, 
                line=dict(width=2, color='DarkSlateGrey')
            ),
            textposition='top center'
        )
        
        fig.update_layout(
            plot_bgcolor='#f8fafc',
            paper_bgcolor='white',
            title_font_size=18,
            title_font_color='#1e3a8a',
            xaxis_tickangle=-45,
            xaxis_title_font=dict(size=14, color='#334155'),
            yaxis_title_font=dict(size=14, color='#334155'),
            xaxis_tickfont=dict(size=10, color='#475569'),
            yaxis_tickfont=dict(size=12, color='#475569'),
            showlegend=True if color_col else False,
            legend_title_text='Marque' if color_col else None,
            height=500,
            margin=dict(t=80, b=150)  # Plus de marge pour les labels
        )
        
        # Ajout de grille pour lisibilité
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#e2e8f0')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#e2e8f0')
        
        # Nettoyage
        plot_df.drop(columns=['_price_num', 'product_short'], inplace=True, errors='ignore')
        
        return fig
    
    def _create_kpi_cards(self) -> Dict[str, Any]:
        """
        Crée des cartes KPI récapitulatives.
        """
        kpis = {}
        
        # Nombre total de produits
        kpis['total_produits'] = len(self.df)
        
        # Nombre de marques uniques
        if self.brand_cols:
            kpis['nb_marques'] = self.df[self.brand_cols[0]].nunique()
        
        # Nombre de catégories
        if self.category_cols:
            kpis['nb_categories'] = self.df[self.category_cols[0]].nunique()
        
        # Prix moyen et total (avec nettoyage)
        if self.price_cols:
            prices_clean = self._clean_price_series(self.price_cols[0]).dropna()
            if len(prices_clean) > 0:
                kpis['prix_moyen'] = prices_clean.mean()
                kpis['prix_total'] = prices_clean.sum()
        
        # Quantité totale
        if self.quantity_cols:
            quantities = pd.to_numeric(self.df[self.quantity_cols[0]], errors='coerce')
            kpis['stock_total'] = quantities.sum()
        
        return kpis


def render_visualizations(df: pd.DataFrame, schema: Dict):
    """
    Fonction helper pour Streamlit : affiche toutes les visualisations.
    
    Args:
        df (pd.DataFrame): Les données
        schema (dict): Le schéma détecté
    """
    # Instanciation du générateur
    viz_gen = AutoVizGenerator(df, schema)
    
    # Génération de toutes les figures
    figures = viz_gen.generate_all_visualizations()
    
    # Affichage des KPI en premier
    if 'kpi_cards' in figures:
        kpis = figures['kpi_cards']
        
        st.subheader("🎯 Indicateurs Clés")
        
        # Déterminer le nombre de colonnes selon les KPI disponibles
        nb_kpis = len(kpis)
        cols = st.columns(min(nb_kpis, 4))
        
        idx = 0
        for key, value in kpis.items():
            with cols[idx % len(cols)]:
                if key == 'total_produits':
                    st.metric("📦 Produits totaux", f"{int(value):,}")
                elif key == 'nb_marques':
                    st.metric("🏷️ Marques", f"{int(value)}")
                elif key == 'nb_categories':
                    st.metric("📂 Catégories", f"{int(value)}")
                elif key == 'prix_moyen':
                    st.metric("💰 Prix moyen", f"{value:,.2f} €")
                elif key == 'prix_total':
                    st.metric("💎 Valeur totale", f"{value:,.2f} €")
                elif key == 'stock_total':
                    st.metric("📊 Stock total", f"{int(value):,} unités")
            idx += 1    
        
        st.divider()
    
    # Organisation des graphiques en onglets pour plus de clarté
    tab_names = []
    tab_figures = []
    
    if 'repartition_marques' in figures:
        tab_names.append("🏷️ Marques")
        tab_figures.append(figures['repartition_marques'])
    
    if 'repartition_categories' in figures:
        tab_names.append("📂 Catégories")
        tab_figures.append(figures['repartition_categories'])
    
    if 'distribution_prix' in figures or 'top_produits_chers' in figures:
        tab_names.append("💰 Prix")
        # On combine les deux graphiques prix dans le même onglet
        has_price_dist = 'distribution_prix' in figures
        has_top_expensive = 'top_produits_chers' in figures
        
        if has_price_dist and has_top_expensive:
            # Créer un conteneur pour les deux graphiques
            def render_price_tab():
                st.plotly_chart(figures['distribution_prix'], use_container_width=True)
                st.divider()
                st.plotly_chart(figures['top_produits_chers'], use_container_width=True)
            tab_figures.append(render_price_tab)
        elif has_price_dist:
            tab_figures.append(figures['distribution_prix'])
        else:
            tab_figures.append(figures['top_produits_chers'])
    
    if 'indicateurs_stock' in figures:
        tab_names.append("📦 Stock")
        tab_figures.append(figures['indicateurs_stock'])
    
    if 'vue_produit_prix' in figures:
        tab_names.append("💎 Analyse")
        tab_figures.append(figures['vue_produit_prix'])
    
    # Affichage des onglets s'il y a des graphiques
    if tab_names:
        st.subheader("📈 Visualisations Automatiques")
        st.caption("Générées automatiquement selon la structure de vos données")
        
        tabs = st.tabs(tab_names)
        
        for i, (tab, fig) in enumerate(zip(tabs, tab_figures)):
            with tab:
                if callable(fig):
                    # Si c'est une fonction (cas du double graphique prix)
                    fig()
                else:
                    # Sinon c'est une figure Plotly
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("ℹ️ Pas assez de données typées pour générer des visualisations automatiques.")


def suggest_insights(df: pd.DataFrame, schema: Dict) -> List[str]:
    """
    Génère des insights textuels automatiques basés sur les données.
    
    Args:
        df (pd.DataFrame): Les données
        schema (dict): Le schéma détecté
    
    Returns:
        list: Liste d'insights sous forme de strings
    """
    insights = []
    
    # Analyse des marques
    brand_cols = [col for col, meta in schema.items() if meta['detected_type'] == 'brand']
    if brand_cols:
        brand_col = brand_cols[0]
        top_brand = df[brand_col].value_counts().index[0]
        top_brand_count = df[brand_col].value_counts().iloc[0]
        total = len(df)
        percentage = (top_brand_count / total) * 100
        
        insights.append(f"🏆 **Marque dominante** : {top_brand} représente {percentage:.1f}% de vos produits ({top_brand_count}/{total})")
    
    # Analyse des prix
    price_cols = [col for col, meta in schema.items() if meta['detected_type'] == 'price']
    if price_cols:
        price_col = price_cols[0]
        prices = pd.to_numeric(df[price_col], errors='coerce').dropna()
        
        if len(prices) > 0:
            avg_price = prices.mean()
            max_price = prices.max()
            min_price = prices.min()
            
            insights.append(f"💰 **Fourchette de prix** : De {min_price:,.2f}€ à {max_price:,.2f}€ (moyenne: {avg_price:,.2f}€)")
            
            # Détection d'anomalies
            q75 = prices.quantile(0.75)
            q25 = prices.quantile(0.25)
            iqr = q75 - q25
            outliers = prices[(prices < q25 - 1.5*iqr) | (prices > q75 + 1.5*iqr)]
            
            if len(outliers) > 0:
                insights.append(f"⚠️ **Anomalies détectées** : {len(outliers)} produit(s) ont un prix atypique (hors de la fourchette normale)")
    
    # Analyse des catégories
    category_cols = [col for col, meta in schema.items() if meta['detected_type'] == 'category']
    if category_cols:
        cat_col = category_cols[0]
        nb_cats = df[cat_col].nunique()
        insights.append(f"📂 **Diversité** : Vos produits sont répartis dans {nb_cats} catégories distinctes")
    
    # Analyse des quantités
    quantity_cols = [col for col, meta in schema.items() if meta['detected_type'] == 'quantity']
    if quantity_cols:
        qty_col = quantity_cols[0]
        quantities = pd.to_numeric(df[qty_col], errors='coerce').dropna()
        
        if len(quantities) > 0:
            total_qty = quantities.sum()
            insights.append(f"📊 **Stock global** : {int(total_qty):,} unités en inventaire")
    
    return insights
