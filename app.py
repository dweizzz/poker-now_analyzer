import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import subprocess
import os
from analytics import PokerAnalytics

st.set_page_config(page_title="PokerNow Analytics MVP", layout="wide")

# Auto-ingestion logic
@st.cache_resource
def run_ingestion():
    input_dir = 'data/to_be_ingested'
    # Ensure directory exists just in case
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs('data/ingested', exist_ok=True)

    # Check if there are any json files to ingest
    has_files = any(f.endswith('.json') for f in os.listdir(input_dir)) if os.path.exists(input_dir) else False

    if has_files:
        with st.spinner("New logs detected. Ingesting to database..."):
            subprocess.run(["python3", "ingest.py", input_dir])

            from analytics import PokerAnalytics
            try:
                PokerAnalytics('pokernow.db').calculate_and_store_player_priors()
            except Exception as e:
                print("Error calculating priors:", e)

    return has_files

# Run it
new_data_processed = run_ingestion()

if new_data_processed:
    st.cache_resource.clear()
    # Need to rerun to clear the caching states properly for other functions
    st.rerun()

st.title("PokerNow Analytics Dashboard")

def get_analytics():
    return PokerAnalytics('pokernow.db')

analytics = get_analytics()

priors_df = analytics.get_priors()

st.sidebar.header("Navigation")
view_mode = st.sidebar.radio("Select View", ["Exploit Dashboard", "Player Profile", "Net PnL Leaderboard"])

if view_mode == "Exploit Dashboard":
    st.header("Opponent Intelligence & Exploit Dashboard")

    st.subheader("Target List")
    targets_df = analytics.get_exploit_targets()
    if not targets_df.empty:
        display_targets = targets_df[['display_name', 'player_id', 'total_hands', 'wtsd_pct', 'wsd_pct', 'wwsf_pct', 'river_bluff_freq', 'profile_tag']].rename(columns={
            'display_name': 'Name',
            'player_id': 'Player ID',
            'total_hands': 'Hands',
            'wtsd_pct': 'WTSD%',
            'wsd_pct': 'WSD%',
            'wwsf_pct': 'WWSF%',
            'river_bluff_freq': 'River Bluff%',
            'profile_tag': 'Profile Tag'
        })
        st.dataframe(
            display_targets.style.format({
                "WTSD%": "{:.1f}%",
                "WSD%": "{:.1f}%",
                "WWSF%": "{:.1f}%",
                "River Bluff%": "{:.1f}%"
            }).background_gradient(subset=["WTSD%", "WWSF%"], cmap="Oranges"),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No sufficient data for Target List (requires >10 hands per player).")

    st.divider()

    st.subheader("Hero Leak Finder")
    hero_leaks = analytics.get_hero_leaks()
    if hero_leaks:
        hero = hero_leaks["stats"]
        leaks = hero_leaks["leaks"]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Hero WTSD%", f"{hero['wtsd_pct']}%")
        col2.metric("Hero WSD%", f"{hero['wsd_pct']}%")
        col3.metric("Hero WWSF%", f"{hero['wwsf_pct']}%")
        col4.metric("Hero River Bluff%", f"{hero['river_bluff_freq']}%")

        st.write("### Analysis")
        for leak in leaks:
            if "Optimal" in leak and not "solid" in leak.lower():
                st.warning(leak)
            else:
                st.success(leak)
    else:
        st.info("No data for Hero ID 'EJd9KHwjJa'.")

elif view_mode == "Net PnL Leaderboard":
    st.header("Net PnL Leaderboard")
    pnl_df = analytics.get_net_pnl_all_players()
    if not pnl_df.empty:
        # Clean dataframe for display
        display_df = pnl_df.rename(columns={
            'player_id': 'Player ID',
            'display_name': 'Name',
            'total_net_pnl': 'Net PnL'
        })
        st.dataframe(
            display_df.style.format({"Net PnL": "{:.2f}"})
                            .background_gradient(subset=["Net PnL"], cmap="RdYlGn"),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No PnL data available.")

elif view_mode == "Player Profile":
    st.sidebar.header("Player Search")
    if not priors_df.empty:
        # Create a mapping dictionary of formatted name -> player_id
        player_options = {}
        for _, row in priors_df.iterrows():
            display_label = f"{row['display_name']} ({row['player_id']})"
            player_options[display_label] = row['player_id']

        selected_label = st.sidebar.selectbox("Select a Player", list(player_options.keys()))

        if selected_label:
            selected_player_id = player_options[selected_label]
            # We can extract just the display name for the headers
            display_name = selected_label.rsplit(" (", 1)[0]
            st.header(f"Player Profile: {display_name}")

            # 1. Priors Section
            st.subheader("Priors (Preflop Statistics)")
        player_priors = priors_df[priors_df['player_id'] == selected_player_id].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Hands", int(player_priors['total_hands']))
        col2.metric("VPIP %", f"{player_priors['vpip_pct']}%")
        col3.metric("PFR %", f"{player_priors['pfr_pct']}%")
        col4.metric("3-Bet %", f"{player_priors['three_bet_pct']}%")

        st.divider()

        # 2. Bet Sizing Frequencies
        st.subheader("Post-Flop Bet-Sizing Frequencies")
        bet_sizing_df = analytics.get_bet_sizing_frequencies()
        if not bet_sizing_df.empty and selected_player_id in bet_sizing_df.index:
            player_bets = bet_sizing_df.loc[[selected_player_id]]
            st.bar_chart(player_bets.T)
        else:
            st.info("No post-flop bets recorded for this player.")

        st.divider()

        # 3. Positional Heatmap
        st.subheader(f"Positional Profit/Loss Heatmap for {display_name}")
        pl_df = analytics.get_profit_loss_by_position(selected_player_id)

        if not pl_df.empty:
            # We want to display this as a heatmap. So we format it into a 1-row pivot
            pl_df = pl_df.set_index('position').T

            fig, ax = plt.subplots(figsize=(8, 2))
            sns.heatmap(pl_df, annot=True, cmap="RdYlGn", center=0, cbar=True, ax=ax, fmt=".1f")
            ax.set_yticklabels(["Net Profit"], rotation=0)
            ax.set_xlabel("Table Position")
            st.pyplot(fig)
        else:
            st.info("No positional profit/loss data available for this player.")

        st.divider()

        # 4. Positional Stats (VPIP, PFR, 3-Bet)
        st.subheader("Positional Preflop Statistics")
        pos_stats_df = analytics.get_positional_stats(selected_player_id)
        if not pos_stats_df.empty:
            display_pos_df = pos_stats_df[['position', 'total_hands', 'vpip_pct', 'pfr_pct', 'three_bet_pct']].rename(columns={
                'position': 'Position',
                'total_hands': 'Hands',
                'vpip_pct': 'VPIP %',
                'pfr_pct': 'PFR %',
                'three_bet_pct': '3-Bet %'
            })
            st.dataframe(
                display_pos_df.style.format({
                    "VPIP %": "{:.1f}%",
                    "PFR %": "{:.1f}%",
                    "3-Bet %": "{:.1f}%"
                }).background_gradient(subset=["VPIP %", "PFR %", "3-Bet %"], cmap="Blues"),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No positional preflop statistics available.")

        st.divider()

        # 5. Hand PNL (Preflop Chart)
        st.subheader("Profit/Loss by Hole Cards (Preflop Chart)")
        hand_pnl_df = analytics.get_pnl_by_hand(selected_player_id)
        if not hand_pnl_df.empty:
            ranks = ['A', 'K', 'Q', 'J', 'T', '9', '8', '7', '6', '5', '4', '3', '2']
            pnl_matrix = pd.DataFrame(index=ranks, columns=ranks, data=np.nan)

            # Populate matrix
            for _, row in hand_pnl_df.iterrows():
                combo = row['hand_combo']
                pnl = row['total_pnl']
                if combo == 'Unknown' or len(combo) not in [2, 3]:
                    continue
                r1, r2 = combo[0], combo[1]
                if len(combo) == 2:
                    pnl_matrix.loc[r1, r2] = pnl
                elif combo[2] == 's':
                    pnl_matrix.loc[r1, r2] = pnl
                elif combo[2] == 'o':
                    pnl_matrix.loc[r2, r1] = pnl

            # Generate annotations
            annot_matrix = pd.DataFrame(index=ranks, columns=ranks, data="")
            for r in ranks:
                for c in ranks:
                    val = pnl_matrix.loc[r, c]
                    combo_name = ""
                    if r == c: combo_name = f"{r}{c}"
                    elif ranks.index(r) < ranks.index(c): combo_name = f"{r}{c}s"
                    else: combo_name = f"{c}{r}o"

                    if pd.isna(val):
                        annot_matrix.loc[r, c] = combo_name
                    else:
                        annot_matrix.loc[r, c] = f"{combo_name}\n{val:.0f}"

            fig, ax = plt.subplots(figsize=(10, 10))
            sns.heatmap(pnl_matrix, annot=annot_matrix, fmt="", cmap="RdYlGn", center=0,
                        cbar_kws={'label': 'Net PnL'}, ax=ax, linewidths=0.5, linecolor='gray',
                        annot_kws={"size": 8})

            # Move x-axis labels to top
            ax.xaxis.tick_top()
            ax.xaxis.set_label_position('top')
            ax.set_aspect('equal')
            st.pyplot(fig)

            # Optional: Still show top/bottom 5 as a small table below
            st.write("---")
            display_hand_df = hand_pnl_df.rename(columns={
                'hand_combo': 'Hand',
                'times_dealt': 'Times Dealt',
                'total_pnl': 'Net PnL'
            })
            display_hand_df = display_hand_df[display_hand_df['Hand'] != 'Unknown']
            col_a, col_b = st.columns(2)
            with col_a:
                st.write("**Top 5 Most Profitable**")
                st.dataframe(
                    display_hand_df.head(5).style.format({"Net PnL": "{:.2f}"}).background_gradient(subset=["Net PnL"], cmap="RdYlGn"),
                    use_container_width=True,
                    hide_index=True
                )
            with col_b:
                st.write("**Bottom 5 Most Profitable**")
                st.dataframe(
                    display_hand_df.tail(5).sort_values('Net PnL', ascending=True).style.format({"Net PnL": "{:.2f}"}).background_gradient(subset=["Net PnL"], cmap="RdYlGn"),
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.info("No hole card PNL data available.")

    else:
        st.warning("No data found in database. Please run ingest.py first.")
